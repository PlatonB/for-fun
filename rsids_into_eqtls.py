__version__ = 'v0.1-beta'

def add_args(ver):
        arg_parser = ArgumentParser(description=f'''
Description: this program adds rsIDs column
into GTEx signif_variant_gene_pairs tables
Dependencies: Pysam
Author: Platon Bykadorov (platon.work@gmail.com), 2021
Version: {ver}
License: GNU General Public License version 3

To download eQTLs and dbSNP, I recommend the download_bio_data tool. It is part
of high-perf-bio project (https://github.com/PlatonB/high-perf-bio). Syntax:
python download_bio_data.py -T trg_dir_path --ciseqtls-gtex --vars-dbsnp -i

cis-eQTL data:
https://storage.googleapis.com/gtex_analysis_v8/single_tissue_qtl_data/GTEx_Analysis_v8_eQTL.tar
Use *signif* files

dbSNP data:
http://ftp.ensembl.org/pub/release-103/variation/vcf/homo_sapiens/
Use homo_sapiens-chr* files

Known problems:
- rsIDs are detected not for all indels.
- program may not correctly select
rsIDs for two-nucleotide indels.
''',
                                    formatter_class=RawTextHelpFormatter,
                                    add_help=False)
        hlp_grp = arg_parser.add_argument_group('Help argument')
        hlp_grp.add_argument('-h', '--help', action='help',
                             help='show this help message and exit')
        man_grp = arg_parser.add_argument_group('Mandatory arguments')
        man_grp.add_argument('-E', '--eqtl-dir-path', required=True, metavar='str', dest='eqtl_dir_path', type=str,
                             help='GTEx eQTL folder')
        man_grp.add_argument('-D', '--dbsnp-dir-path', required=True, metavar='str', dest='dbsnp_dir_path', type=str,
                             help='dbSNP folder')
        man_grp.add_argument('-T', '--trg-dir-path', required=True, metavar='str', dest='trg_dir_path', type=str,
                             help='target folder')
        opt_grp = arg_parser.add_argument_group('Optional arguments')
        opt_grp.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                             help='maximum number of eQTL files to modify in parallel')
        args = arg_parser.parse_args()
        return args

class PrepSingleProc():
        def __init__(self, args):
                self.eqtl_dir_path = os.path.normpath(args.eqtl_dir_path)
                self.dbsnp_dir_path = os.path.normpath(args.dbsnp_dir_path)
                self.trg_dir_path = os.path.normpath(args.trg_dir_path)
                self.eqtl_file_names = os.listdir(self.eqtl_dir_path)
                self.dbsnp_file_names = list(filter(lambda dbsnp_file_name: '.csi' not in dbsnp_file_name,
                                                    os.listdir(self.dbsnp_dir_path)))
        def mod_eqtl_file(self, eqtl_file_name):
                with ExitStack() as stack:
                        dbsnp_files_opened = {re.search(r'chr(?:\d{1,2}|X|Y|MT)', dbsnp_file_name).group():
                                              stack.enter_context(VariantFile(os.path.join(self.dbsnp_dir_path,
                                                                                           dbsnp_file_name))) for dbsnp_file_name in self.dbsnp_file_names}
                        eqtl_file_path = os.path.join(self.eqtl_dir_path, eqtl_file_name)
                        trg_file_name = eqtl_file_name.rsplit('.', maxsplit=2)[0] + 'rsIDs.tsv.gz'
                        trg_file_path = os.path.join(self.trg_dir_path, trg_file_name)
                        with gzip.open(eqtl_file_path, mode='rt') as eqtl_file_opened:
                                with gzip.open(trg_file_path, mode='wt') as trg_file_opened:
                                        trg_file_opened.write('rsID\t' + eqtl_file_opened.readline())
                                        for eqtl_line in eqtl_file_opened:
                                                eqtl_row = eqtl_line.split('\t')
                                                eqtl_chrom, eqtl_pos, eqtl_ref, eqtl_alt = eqtl_row[0].split('_')[:-1]
                                                eqtl_pos = int(eqtl_pos)
                                                if len(eqtl_ref) == len(eqtl_alt):
                                                        for dbsnp_rec in dbsnp_files_opened[eqtl_chrom].fetch(eqtl_chrom.replace('chr', ''),
                                                                                                              eqtl_pos - 1,
                                                                                                              eqtl_pos):
                                                                if dbsnp_rec.alts is None:
                                                                        continue
                                                                if eqtl_ref == dbsnp_rec.ref and eqtl_alt in dbsnp_rec.alts:
                                                                        eqtl_row.insert(0, dbsnp_rec.id)
                                                                        break
                                                        else:
                                                                eqtl_row.insert(0, '.')
                                                elif len(eqtl_ref) > len(eqtl_alt):
                                                        for dbsnp_rec in dbsnp_files_opened[eqtl_chrom].fetch(eqtl_chrom.replace('chr', ''),
                                                                                                              eqtl_pos,
                                                                                                              eqtl_pos + 1):
                                                                if dbsnp_rec.alts is None:
                                                                        continue
                                                                if eqtl_ref[1:] == dbsnp_rec.ref[:-1]:
                                                                        eqtl_row.insert(0, dbsnp_rec.id)
                                                                        break
                                                        else:
                                                                eqtl_row.insert(0, '.')
                                                elif len(eqtl_ref) < len(eqtl_alt):
                                                        for dbsnp_rec in dbsnp_files_opened[eqtl_chrom].fetch(eqtl_chrom.replace('chr', ''),
                                                                                                              eqtl_pos,
                                                                                                              eqtl_pos + 1):
                                                                if dbsnp_rec.alts is None:
                                                                        continue
                                                                if eqtl_alt[1:] in map(lambda dbsnp_alt: dbsnp_alt[:-1], dbsnp_rec.alts):
                                                                        eqtl_row.insert(0, dbsnp_rec.id)
                                                                        break
                                                        else:
                                                                eqtl_row.insert(0, '.')
                                                trg_file_opened.write('\t'.join(eqtl_row))
                                                
####################################################################################################

import os, datetime, re, gzip
from argparse import ArgumentParser, RawTextHelpFormatter
from multiprocessing import Pool
from contextlib import ExitStack
from pysam import VariantFile

args = add_args(__version__)
max_proc_quan = args.max_proc_quan
prep_single_proc = PrepSingleProc(args)
eqtl_file_names = prep_single_proc.eqtl_file_names
eqtl_files_quan = len(eqtl_file_names)
if max_proc_quan > eqtl_files_quan <= 8:
        proc_quan = eqtl_files_quan
elif max_proc_quan > 8:
        proc_quan = 8
else:
        proc_quan = max_proc_quan
        
print(f'\nModifying eQTL data')
print(f'\tnumber of parallel processes: {proc_quan}')

with Pool(proc_quan) as pool_obj:
        exec_time_start = datetime.datetime.now()
        pool_obj.map(prep_single_proc.mod_eqtl_file, eqtl_file_names)
        exec_time = datetime.datetime.now() - exec_time_start
        
print(f'\tparallel computation time: {exec_time}')
