from datetime import datetime


class Logger:
    def __init__(self, name):
        """
        For saving name log's file
        :param name: String
        """
        self.__name = name

    def write_log(self, text_log):
        """
        For writing log in file
        :param text_log: String with Error or Successfully
        :return: None
        """
        name_out_file = './' + self.__name + datetime.now().strftime("%d%m%Y") + 'log.txt'
        try:
            with open(name_out_file, 'a') as f:
                f.write(datetime.now().strftime("%H:%M:%S") + ' ' + text_log + '\n')
        except:
            pass
