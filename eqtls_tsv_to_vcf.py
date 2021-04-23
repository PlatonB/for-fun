__version__ = 'v1.0'

def add_args(ver):
        arg_parser = ArgumentParser(description=f'''
Description: this program converts GTEx cis-eQTL files from TSV to VCF
Dependencies: -
Author: Platon Bykadorov (platon.work@gmail.com), 2021
Version: {ver}
License: GNU General Public License version 3

To download eQTLs, I recommend the download_bio_data tool. It is part of
high-perf-bio project (https://github.com/PlatonB/high-perf-bio). Syntax:
python download_bio_data.py -T trg_dir_path --ciseqtls-gtex

cis-eQTL data:
https://storage.googleapis.com/gtex_analysis_v8/single_tissue_qtl_data/GTEx_Analysis_v8_eQTL.tar
Use only *signif* files
''',
                                    formatter_class=RawTextHelpFormatter,
                                    add_help=False)
        hlp_grp = arg_parser.add_argument_group('Help argument')
        hlp_grp.add_argument('-h', '--help', action='help',
                             help='show this help message and exit')
        man_grp = arg_parser.add_argument_group('Mandatory arguments')
        man_grp.add_argument('-S', '--src-dir-path', required=True, metavar='str', dest='src_dir_path', type=str,
                             help='GTEx eQTL folder')
        man_grp.add_argument('-T', '--trg-dir-path', required=True, metavar='str', dest='trg_dir_path', type=str,
                             help='target folder')
        opt_grp = arg_parser.add_argument_group('Optional arguments')
        opt_grp.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                             help='maximum number of eQTL files to convert in parallel')
        args = arg_parser.parse_args()
        return args

class PrepSingleProc():
        def __init__(self, args):
                self.src_dir_path = os.path.normpath(args.src_dir_path)
                self.trg_dir_path = os.path.normpath(args.trg_dir_path)
                self.src_file_names = os.listdir(self.src_dir_path)
        def convert(self, src_file_name):
                src_file_path = os.path.join(self.src_dir_path, src_file_name)
                trg_file_name = src_file_name.rsplit('.', maxsplit=2)[0] + '.vcf.gz'
                trg_file_path = os.path.join(self.trg_dir_path, trg_file_name)
                with gzip.open(src_file_path, mode='rt') as src_file_opened:
                        with gzip.open(trg_file_path, mode='wt') as trg_file_opened:
                                src_header_row = src_file_opened.readline().rstrip().split('\t')
                                trg_file_opened.write('##fileformat=VCFv4.3\n')
                                trg_file_opened.write('#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n')
                                for src_line in src_file_opened:
                                        src_row = src_line.rstrip().split('\t')
                                        src_varid_row = src_row[0].split('_')
                                        trg_info = ';'.join(map(lambda key, val:
                                                                f'{key}={val}',
                                                                src_header_row[1:],
                                                                src_row[1:]))
                                        trg_row = [src_varid_row[0][3:],
                                                   src_varid_row[1],
                                                   '.',
                                                   src_varid_row[2],
                                                   src_varid_row[3],
                                                   '.',
                                                   '.',
                                                   trg_info]
                                        trg_file_opened.write('\t'.join(trg_row) + '\n')
                                        
####################################################################################################

import os, datetime, gzip
from argparse import ArgumentParser, RawTextHelpFormatter
from multiprocessing import Pool

args = add_args(__version__)
max_proc_quan = args.max_proc_quan
prep_single_proc = PrepSingleProc(args)
src_file_names = prep_single_proc.src_file_names
src_files_quan = len(src_file_names)
if max_proc_quan > src_files_quan <= 8:
        proc_quan = src_files_quan
elif max_proc_quan > 8:
        proc_quan = 8
else:
        proc_quan = max_proc_quan
        
print(f'\nConverting eQTL data')
print(f'\tnumber of parallel processes: {proc_quan}')

with Pool(proc_quan) as pool_obj:
        exec_time_start = datetime.datetime.now()
        pool_obj.map(prep_single_proc.convert, src_file_names)
        exec_time = datetime.datetime.now() - exec_time_start
        
print(f'\tparallel computation time: {exec_time}')
