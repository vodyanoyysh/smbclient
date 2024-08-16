import io
import time
import socket
from tempfile import TemporaryFile

from pydantic import BaseModel, Field
from smb.SMBConnection import SMBConnection


class MasterNode(BaseModel):
    host: str = Field(None, description="SMB хост")
    service_name: str = Field(None, description="Сервис")
    username: str = Field(None, description="Имя пользователя")
    password: str = Field(None, description="Пароль")


class BackupNode(BaseModel):
    host: str = Field(None, description="SMB хост")
    service_name: str = Field(None, description="Сервис")
    username: str = Field(None, description="Имя пользователя")
    password: str = Field(None, description="Пароль")


class SMBConfig(BaseModel):
    master_node: MasterNode = Field(None, description="Конфигурация основного подключения")
    backup_node: BackupNode = Field(None, description="Конфигурация запасного подключения")
    reconnect_wait_time: int = Field(None, description="Время ожидания переподключения в секундах")
    reconnect_attempts: int = Field(None, description="Количество попыток переподключения")


class SMB:
    def __init__(self, cfg, log):
        self.log = log
        self.cfg = SMBConfig(**cfg)
        self.current_connection: SMBConnection | None = None
        self.service_name = ""
        self.connect()

    def __del__(self):
        self.current_connection.close()
        self.current_connection = None

    def connect(self):
        def __connect_master():
            try:
                connection = SMBConnection(self.cfg.master_node.username, self.cfg.master_node.password, socket.gethostname(),
                                           self.cfg.master_node.host, domain="group.s7", use_ntlm_v2=True, is_direct_tcp=True)
                connection.connect(socket.gethostbyname(self.cfg.master_node.host), 445)
                try:
                    connection.listPath(self.cfg.master_node.service_name, "")
                    self.current_connection = connection
                    self.service_name = self.cfg.master_node.service_name
                except Exception as err:
                    raise err
                return True
            except Exception as err:
                self.log.error(err)
                return False

        def __connect_backup():
            try:
                connection = SMBConnection(self.cfg.backup_node.username, self.cfg.backup_node.password, socket.gethostname(),
                                           self.cfg.backup_node.host, domain="group.s7", use_ntlm_v2=True, is_direct_tcp=True)
                connection.connect(socket.gethostbyname(self.cfg.backup_node.host), 445)
                try:
                    connection.listPath(self.cfg.backup_node.service_name, "")
                    self.current_connection = connection
                    self.service_name = self.cfg.backup_node.service_name
                except Exception as err:
                    raise err
                return True
            except Exception as err:
                self.log.error(err)
                return False

        try_count: int = 1
        while not self.current_connection:
            if try_count >= self.cfg.reconnect_attempts:
                self.log.info(
                    f"failed to connect to {self.cfg.master_node.host}:{self.cfg.master_node.service_name} try connect to {self.cfg.backup_node.host}:{self.cfg.backup_node.service_name}")
                if __connect_backup():
                    self.log.info(f"successfully connected to {self.cfg.backup_node.host}:{self.cfg.backup_node.username}")
            elif __connect_master():
                self.log.info(f"successfully connected to {self.cfg.master_node.host}:{self.cfg.master_node.service_name}")
            else:
                self.log.error(f"failed to connect to {self.cfg.master_node.host}:{self.cfg.master_node.service_name}. try reconnecting...")
            try_count += 1
            time.sleep(self.cfg.reconnect_wait_time)

    def check_connection(self):
        if self.current_connection.remote_name.upper() == self.cfg.master_node.host.upper():
            try:
                self.current_connection.listPath(self.cfg.master_node.service_name, "")
                return True
            except Exception as err:
                self.current_connection = None
                self.connect()
                if self.current_connection:
                    return True
                return False
        elif self.current_connection.remote_name.upper() == self.cfg.backup_node.host.upper():
            try:
                self.current_connection.listPath(self.cfg.backup_node.service_name, "")
                return True
            except Exception as err:
                self.current_connection = None
                self.connect()
                if self.current_connection:
                    return True
                return False

    def ls(self, dir_path: str, regex="*", sort_order="desc") -> list:
        """
        Вывести список файлов в директории
        :param dir_path: путь до директории
        :param regex: регулярное выражение для фильтрации имен файлов
        :param sort_order: порядок сортировки ('asc' - по возрастанию, 'desc' - по убыванию)
        :return: список имен файлов в указанной директории
        """
        if self.check_connection():
            files_list = self.current_connection.listPath(self.service_name, dir_path, pattern=regex)
            files_list.sort(key=lambda x: x.create_time, reverse=(sort_order == "desc"))
            filenames_list = [file.filename for file in files_list]
            return filenames_list
        else:
            raise Exception("not available connection")

    def check_file_in_directory(self, dir_path: str, file_name: str) -> bool:
        """
        Проверить существование файла в директории
        :param dir_path: путь до директории
        :param file_name: имя проверяемого файла
        :return: bool
        """
        if not self.check_connection():
            self.connect()
        return file_name in self.ls(dir_path)

    def upload_bytes(self, dir_path: str, file_name: str, payload: io.BytesIO):
        """
        Загрузить файл в директорию
        :param dir_path: путь до директории
        :param file_name: имя загружаемого файла
        :param payload: io.BytesIO("test".encode("utf-8"))
        :return:
        """
        try:
            if not self.check_connection():
                self.connect()
            self.current_connection.storeFile(self.service_name, f"{dir_path}/{file_name}", payload)
            return True
        except Exception as err:
            self.log.error(err)
            return False

    def download_bytes(self, dir_path: str, file_name: str) -> io.BufferedRandom:
        """
        Загрузить файл из директории в io.BytesIO
        :param dir_path: путь до директории
        :param file_name: имя загружаемого файла
        :return: io.BytesIO("test".encode("utf-8"))
        """
        try:
            if not self.check_connection():
                self.connect()
            file_data = TemporaryFile()
            self.current_connection.retrieveFile(self.service_name, f"{dir_path}/{file_name}", file_data)
            file_data.seek(0)
            return file_data
        except Exception as err:
            self.log.error(err)
            raise err

    def delete_file(self, dir_path: str, file_name: str):
        """
        Удалить файл из директории
        :param dir_path: путь до директории
        :param file_name: имя удаляемого файла
        :return:
        """
        try:
            if not self.check_connection():
                self.connect()
            self.current_connection.deleteFiles(self.service_name, f"{dir_path}/{file_name}")
            return True
        except Exception as err:
            self.log.error(err)
            return False

    def move_file(self, path_from: str, path_to: str):
        """
        Переместить файл из одной директории в другую
        :param path_from: путь до исходного файла
        :param path_to: путь до новой директории
        :return:
        """
        try:
            if not self.check_connection():
                self.connect()
            self.current_connection.rename(self.service_name, f"{path_from}", f"{path_to}")
            return True
        except Exception as err:
            self.log.error(err)
            return False
