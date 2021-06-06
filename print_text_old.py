from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from os import environ


class PrintText(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        text_to_print,
        *args, **kwargs):

        super(PrintText, self).__init__(*args, **kwargs)
        self.text_to_print = text_to_print

    def __print_text(self):

        print(text_to_print)

    def execute(self, context):
        self.__print_text()