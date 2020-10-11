__version__ = 'V2.0'

def add_args(ver):
        '''
        Работа с аргументами командной строки. При
        желании, подобные функции можно разносить
        по отдельным модулям. Допустим, если
        хочется запилить CLI на разных языках.
        '''
        argparser = ArgumentParser(description=f'''
Скелет программы, распределяющей обработку
разных текстовых файлов по разным процессам.

Версия: {ver}
Требуемые сторонние компоненты: -
Автор: Платон Быкадоров (platon.work@gmail.com), 2020
Лицензия: GNU General Public License version 3
Поддержать проект: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Документация: -
Багрепорты, пожелания, общение: https://github.com/PlatonB/for-fun/issues

Программа в тестовых целях
тупо копирует первый столбец
каждой таблицы в отдельный файл.

Уверен, что вы сможете написать по
этому шаблону супер-полезный код!

Условные обозначения в справке по CLI:
- краткая форма с большой буквы: обязательный аргумент;
- в квадратных скобках: значение по умолчанию;
- в фигурных скобках: перечисление возможных значений.
''',
                                   formatter_class=RawTextHelpFormatter)
        argparser.add_argument('-S', '--src-dir-path', metavar='str', dest='src_dir_path', type=str,
                               help='Путь к папке с исходными таблицами')
        argparser.add_argument('-t', '--trg-dir-path', metavar='[None]', dest='trg_dir_path', type=str,
                               help='Путь к папке для результатов (по умолчанию - путь к исходной папке)')
        argparser.add_argument('-m', '--meta-lines-quan', metavar='[0]', default=0, dest='meta_lines_quan', type=int,
                               help='Количество строк метаинформации в начале каждой исходной таблицы (шапку сюда не включайте)')
        argparser.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                               help='Максимальное количество параллельно обрабатываемых таблиц')
        args = argparser.parse_args()
        return args

class PrepSingleProc():
        '''
        Предполагается, что функция, берущая на
        себя всю работу над каждым файлом, должна
        подаваться в map, а значит, принимать
        только имя файла из скармливаемого
        этому map списка таких имён. Как тогда
        передать функции любые другие аргументы?
        Выход из положения - создать класс,
        насытить его нужными атрибутами, заодно
        протащив их в функцию посредством self.
        '''
        
        def __init__(self, args):
                '''
                Поскольку эта программа - лишь шаблон
                для написания настоящих решений, набор
                атрибутов крайне минималистичен. Учтите,
                что изменение атрибутов распараллеливаемым
                методом этого класса, вероятнее всего,
                повергнет выполнение программы в хаос.
                '''
                self.src_dir_path = os.path.normpath(args.src_dir_path)
                if args.trg_dir_path is None:
                        self.trg_dir_path = self.src_dir_path
                else:
                        self.trg_dir_path = os.path.normpath(args.trg_dir_path)
                self.meta_lines_quan = args.meta_lines_quan
                
        def save_fir_col(self, src_file_name):
                '''
                Место для вашей рек^Wвашего кода:).
                '''
                src_file_path = os.path.join(self.src_dir_path, src_file_name)
                trg_file_name = f'res_{src_file_name}'
                trg_file_path = os.path.join(self.trg_dir_path, trg_file_name)
                with open(src_file_path) as src_file_opened:
                        with open(trg_file_path, 'w') as trg_file_opened:
                                for meta_line_index in range(self.meta_lines_quan):
                                        trg_file_opened.write(src_file_opened.readline())
                                for line in src_file_opened:
                                        trg_file_opened.write(line.rstrip().split('\t')[0] + '\n')
                                        
####################################################################################################

import os
from argparse import ArgumentParser, RawTextHelpFormatter
from multiprocessing import Pool

#Вызов конструктора CLI. Если
#исследователь добавит к команде
#-h, то выведется справка, и всё.
args = add_args(__version__)

#Создание экземпляра класса, взращивающего
#метод обработки одного исходного файла.
prep_single_proc = PrepSingleProc(args)

#Ранее запрошенная у исследователя
#верхняя граница количества
#параллельных миров^Wпроцессов.
#Вычисляемое позже реальное
#количество процессов может
#оказаться значительно ниже.
max_proc_quan = args.max_proc_quan

#Построение списка имён исходных
#файлов, который затем должен будет
#перебираться мультипроцессинговым map.
src_file_names = os.listdir(prep_single_proc.src_dir_path)

#Расчёт количества исходных файлов.
#Оно вскоре пригодится для точного
#определения количества процессов.
src_files_quan = len(src_file_names)

#Программа не допустит, чтобы процессов
#было больше, чем обрабатываемых файлов.
#Принимаемая мера при таком раскладе -
#приравнивание первого из перечисленных
#ко второму. Иначе это будет от слова
#бессмысленно до небезопасно. Также
#программа вынуждена пресечь душевный
#порыв исследователя задать как можно
#большее количество процессов:). При
#работе с громадным числом файлов на
#овердофигаядерном сервере, безусловно,
#хочется зверски распараллелить выполнение
#задачи. Почему прога такая злая и не
#даёт большого простора в этой области?
#Дело в том, что тормозящим фактором
#тогда станет езда головки диска.
#Поэтому при солидном количестве
#файлов и заданном с запасом максимуме,
#программа примет решение установить
#мультипроцессовость, равную восьми.
if max_proc_quan > src_files_quan <= 8:
        proc_quan = src_files_quan
elif max_proc_quan > 8:
        proc_quan = 8
else:
        proc_quan = max_proc_quan
        
print(f'\nДеланье серьёзного дела:)')
print(f'\tколичество параллельных процессов: {proc_quan}')

#Собственно, запуск параллельных вычислений.
with Pool(proc_quan) as pool_obj:
        pool_obj.map(prep_single_proc.save_fir_col, src_file_names)
