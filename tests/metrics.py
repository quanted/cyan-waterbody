import unittest


def get_columns():
    detect_columns = [f"DN={i}" for i in range(1, 254)]
    all_columns = [f"DN={i}" for i in range(0, 254)]
    return detect_columns, all_columns

def get_data():
    pass


class MetricsCalculationMethods(unittest.TestCase):

    def test_frequency_calculation(self):
        detect_columns, all_columns = get_columns()
