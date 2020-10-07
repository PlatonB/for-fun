__version__ = 'V1.0'

def get_intgen_data(intgen_ftp_url, intgen_man_url, intgen_dir_path):
        intgen_file_urls = []
        with urllib.request.urlopen(intgen_man_url) as response:
                for byte_line in response:
                        line = byte_line.decode()
                        if 'chr' not in line:
                                continue
                        intgen_file_url = os.path.join(intgen_ftp_url, line.split('\t')[0][2:])
                        intgen_file_urls.append(intgen_file_url)
        for intgen_file_url in intgen_file_urls:
                intgen_file_path = os.path.join(intgen_dir_path, os.path.basename(intgen_file_url))
                if os.path.exists(intgen_file_path) == False:
                        os.system(f'curl -s {intgen_file_url} -o {intgen_file_path}')
                        
def prep_dbsnp_data(dbsnp_ftp_url, dbsnp_dir_path, proc_quan):
        with urllib.request.urlopen(dbsnp_ftp_url) as response:
                dbsnp_vcfgz_names = re.findall(r'homo_sapiens-chr\w+\.vcf\.gz(?=\r\n)',
                                               response.read().decode())
                for dbsnp_vcfgz_name in dbsnp_vcfgz_names:
                        dbsnp_vcfgz_url = os.path.join(dbsnp_ftp_url, dbsnp_vcfgz_name)
                        chr_name = re.search(r'(?<=chr)\w+(?=\.)', dbsnp_vcfgz_name).group()
                        dbsnp_tsvgz_path = os.path.join(dbsnp_dir_path, f'{chr_name}.tsv.gz')
                        if os.path.exists(dbsnp_tsvgz_path) == False:
                                os.system(f'''
curl -s {dbsnp_vcfgz_url} |
zgrep "^[^#]" |
cut -f 1-5 |
bgzip -l 9 -@ {proc_quan} > {dbsnp_tsvgz_path}''')
                        if os.path.exists(f'{dbsnp_tsvgz_path}.tbi') == False:
                                tabix_index(dbsnp_tsvgz_path, preset='vcf')
                                
def mod_intgen_data(intgen_vcfgz_path):
        intgen_vcfgz_name = os.path.basename(intgen_vcfgz_path)
        chr_name = re.search(r'(?<=chr)\w+(?=\.)', intgen_vcfgz_name).group()
        dbsnp_tsvgz_path = os.path.join(args.dbsnp_dir_path, f'{chr_name}.tsv.gz')
        trg_vcf_path = os.path.join(args.trg_dir_path, intgen_vcfgz_name[:-3])
        with TabixFile(intgen_vcfgz_path) as intgen_vcfgz_opened:
                with TabixFile(dbsnp_tsvgz_path) as dbsnp_tsvgz_opened:
                        with open(trg_vcf_path, 'w') as trg_vcf_opened:
                                for meta_line in intgen_vcfgz_opened.header:
                                        trg_vcf_opened.write(meta_line + '\n')
                                for intgen_tup in intgen_vcfgz_opened.fetch(parser=asTuple()):
                                        pos = int(intgen_tup[1])
                                        for dbsnp_tup in dbsnp_tsvgz_opened.fetch(chr_name, pos - 1, pos, parser=asTuple()):
                                                if intgen_tup[3] == dbsnp_tup[3] and intgen_tup[4] in dbsnp_tup[4].split(','):
                                                        intgen_list = list(intgen_tup)
                                                        intgen_list[2] = dbsnp_tup[2]
                                                        trg_vcf_opened.write('\t'.join(intgen_list) + '\n')
                                                        break
                                        else:
                                                trg_vcf_opened.write('\t'.join(intgen_tup) + '\n')
        os.system(f'bgzip -l 9 -i {trg_vcf_path}')
        tabix_index(f'{trg_vcf_path}.gz', preset='vcf')
        
import os, urllib.request, re
from argparse import ArgumentParser, RawTextHelpFormatter
from pysam import tabix_index, TabixFile, asTuple
from multiprocessing import Pool

argparser = ArgumentParser(description=f'''
Description: this program replaces dots with rsIDs in
1000 Genomes 20190312_biallelic_SNV_and_INDEL VCFs
Dependencies: Pysam, HTSlib
Requirements: Python >= 3.6, stable internet connection
Author: Platon Bykadorov (platon.work@gmail.com), 2020
Version: {__version__}
License: GNU General Public License version 3
''',
                           formatter_class=RawTextHelpFormatter)
argparser.add_argument('-I', '--intgen-dir-path', metavar='str', dest='intgen_dir_path', type=str,
                       help='1000 Genomes folder')
argparser.add_argument('-D', '--dbsnp-dir-path', metavar='str', dest='dbsnp_dir_path', type=str,
                       help='dbSNP folder')
argparser.add_argument('-T', '--trg-dir-path', metavar='str', dest='trg_dir_path', type=str,
                       help='target folder')
argparser.add_argument('-p', '--proc-quan', metavar='[4]', default=4, dest='proc_quan', type=int,
                       help='number of parallel processes')
args = argparser.parse_args()

intgen_ftp_url = 'http://ftp.1000genomes.ebi.ac.uk/vol1/ftp/data_collections/1000_genomes_project/release/20190312_biallelic_SNV_and_INDEL/'
intgen_man_url = os.path.join(intgen_ftp_url, '20190312_biallelic_SNV_and_INDEL_MANIFEST.txt')
dbsnp_ftp_url = 'ftp://ftp.ensembl.org/pub/release-101/variation/vcf/homo_sapiens/'

print('1000 Genomes data downloading')
get_intgen_data(intgen_ftp_url, intgen_man_url, args.intgen_dir_path)
print('dbSNP data downloading and processing')
prep_dbsnp_data(dbsnp_ftp_url, args.dbsnp_dir_path, args.proc_quan)
with os.scandir(args.intgen_dir_path) as intgen_dir_opened:
        intgen_vcfgz_paths = [intgen_file_obj.path for intgen_file_obj in intgen_dir_opened if 'tbi' not in intgen_file_obj.name]
print(f'1000 Genomes` ID column modification in {args.proc_quan} process(es)')
with Pool(args.proc_quan) as pool_obj:
        pool_obj.map(mod_intgen_data, intgen_vcfgz_paths)
