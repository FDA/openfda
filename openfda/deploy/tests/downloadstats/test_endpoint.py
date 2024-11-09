from openfda.tests import api_test_helpers
import inspect
import sys


def test_download_counts():
    status = api_test_helpers.json("/usage.json")
    assert status["downloadStats"]["deviceudi"] >= 111000
    assert status["downloadStats"]["deviceenforcement"] >= 288800
    assert status["downloadStats"]["devicereglist"] >= 1999834
    assert status["downloadStats"]["othernsde"] >= 9215
    assert status["downloadStats"]["druglabel"] >= 225705
    assert status["downloadStats"]["covid19serology"] >= 2753
    assert status["downloadStats"]["ndc"] >= 124783
    assert status["downloadStats"]["devicepma"] >= 9783
    assert status["downloadStats"]["deviceclearance"] >= 20863
    assert status["downloadStats"]["tobaccoproblem"] >= 761
    assert status["downloadStats"]["foodenforcement"] >= 80278
    assert status["downloadStats"]["drugsfda"] >= 15972
    assert status["downloadStats"]["foodevent"] >= 16823
    assert status["downloadStats"]["deviceevent"] >= 878275
    assert status["downloadStats"]["animalandveterinarydrugevent"] >= 325266
    assert status["downloadStats"]["othersubstance"] >= 2491
    assert status["downloadStats"]["deviceclass"] >= 91209
    assert status["downloadStats"]["devicerecall"] >= 33674
    assert status["downloadStats"]["drugenforcement"] >= 38296
    assert status["downloadStats"]["drugevent"] >= 3093777


if __name__ == "__main__":
    all_functions = inspect.getmembers(sys.modules[__name__], inspect.isfunction)
    for key, func in all_functions:
        if key.find("test_") > -1:
            func()
