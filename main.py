from ets.ets_ftp_lib import Ftp
from os.path import join
from datetime import datetime, timedelta
from itertools import count
from config import *
from queries import *
from ets.ets_xml_worker import found_procedure_223_db
from ets.ets_mysql_lib import MysqlConnection as Mc
import logger_module
import ets.ets_email_lib as email
import re
import shutil
import os


def main():

    # инициализируем счетчик записей
    counter = count(start=1, step=1)

    # инициализируем подключение к ftp
    connect = Ftp(connection=Ftp.CONNECT_223_FREE_FTP)
    # инициализируем подключения к бд
    cn_223_old = Mc(connection=Mc.MS_94_1_CONNECT)
    cn_catalog = Mc(connection=Mc.MS_223_CATALOG_CONNECT)

    # искать будем за вчерашний день
    YESTERDAY = (datetime.now() - timedelta(days=1)).__format__('%Y%m%d')

    MAIL_TEXT = ''

    # в этот временный файл мы будем писать загруженные архивы
    tmp_zip = join(tmp_dir, zip_file)

    connect.open()

    # сюда мы запишем все локальные пути до xml, которые нам подошли
    files_for_send = []

    for search_dir in search_dirs:

        # собираем содержимое всех указанных директорий (внутри они разбиты по регионам, нам нужно обойти все)
        archive_dirs = [region_dir + search_dir for region_dir in connect.nlst(all_data_dir)]

        # получим полные пути до всех архивов за вчерашний день и зачем то упорядочим
        # просто потому что все должно быть красиво)
        all_new_archives = []
        for archive_dir in archive_dirs:
            region_files = sorted(connect.nlst(archive_dir), key=lambda k: k, reverse=True)
            region_files = filter(lambda k: re.findall(YESTERDAY, k), region_files)
            for file in region_files:
                all_new_archives.append(file)

        # поочередно скачиваем архивы
        for archive in all_new_archives:
            connect.get(tmp_zip, archive)

            # распакуем архив, а его самого удалим
            shutil.unpack_archive(tmp_zip, tmp_dir)
            os.remove(tmp_zip)

            # обрабатываем каждую xml, которая распаковалась в директорию
            for xml_file in os.listdir(tmp_dir):
                file_r = join(tmp_dir + xml_file)
                with open(file_r, mode='r', encoding='utf8') as xml:
                    XML = xml.read()

                # находим условия, которые будем проверять
                OUR_PLACE = re.findall(r'<electronicPlaceId>(104|2580)</electronicPlaceId>', XML)

                # если для нашей площадки и метод не от старой секции, то забираем xml
                if OUR_PLACE:
                    auction = re.findall(r'<ns2:registrationNumber>(.*?)</ns2:registrationNumber>', XML)[0]
                    version = re.findall(r'<ns2:version>(.*?)</ns2:version>', XML)[0]
                    method_name = re.findall(r'<ns2:purchaseCodeName>(.*?)</ns2:purchaseCodeName>', XML)[0]
                    method_code = re.findall(r'<ns2:purchaseMethodCode>(.*?)</ns2:purchaseMethodCode>', XML)[0]
                    number = str(next(counter))
                    publication_info = found_procedure_223_db(auction, version=version)

                    with cn_223_old.open():
                        ON_OLD_223 = cn_223_old.execute_query(check_old_223_query % auction)
                    with cn_catalog.open():
                        ON_CATALOG = cn_catalog.execute_query(check_catalog_query % (auction, version))

                    if publication_info:
                        MAIL_TEXT += '%s) %s версия %s (%s, код %s): опубликован как %s' % (number, auction,
                                                                                            version,
                                                                                            method_name, method_code,
                                                                                            publication_info['name'])
                    else:
                        MAIL_TEXT += '%s) %s версия %s (%s, код %s): не опубликован на площадке' % (number, auction,
                                                                                                    version,
                                                                                                    method_name,
                                                                                                    method_code)

                    if ON_CATALOG:
                        MAIL_TEXT += ', в каталоге присутствует'
                    else:
                        MAIL_TEXT += ', в каталоге отсутствует'

                    if ON_OLD_223:
                        MAIL_TEXT += ', дублируется со старой 223\n\n'
                    else:
                        MAIL_TEXT += ', на старой 223 не публиковался\n\n'

                    xml_with_link = join(found_xml_dir, number + '_' + auction + '_' + version + '.xml')
                    files_for_send.append(xml_with_link)
                    shutil.copyfile(file_r, xml_with_link)

                os.remove(file_r)

    XML_COUNT = len(files_for_send)
    MAIL_THEME = 'Отчет по новым извещениям 223'

    if XML_COUNT:
        MAIL_TEXT = 'Найдены новые извещения (во вложении)\n\n' + MAIL_TEXT
    else:
        MAIL_TEXT = 'Новые извещения отсутствуют'

    connect.close()

    email.mail_sender(MAIL_THEME, MAIL_TEXT,
                      recipients=recipients,
                      add_files=files_for_send,
                      report=True,
                      counter=XML_COUNT,
                      datetime=True)


if __name__ == '__main__':
    logger = logger_module.logger()
    try:
        main()
    # если при исполнении будут исключения - кратко выводим на терминал, остальное - в лог
    except Exception as e:
        logger.fatal('Fatal error! Exit', exc_info=True)
        print('Critical error: %s' % e)
        print('More information in log file')
        exit(1)
exit(0)
