import shutil
from unittest import mock

import pytest


def pytest_runtest_setup(item):
    if "needs_exiftool" in item.keywords and shutil.which("exiftool") is None:
        pytest.skip("exiftool not available")


def pytest_configure(config):
    config.addinivalue_line("markers", "needs_exiftool: requires exiftool executable in PATH")


@pytest.fixture
def mocker(request):
    """Lightweight stand-in when pytest-mock is unavailable."""
    active_patches = []

    class _PatchHelper:
        def __init__(self, parent):
            self._parent = parent

        def __call__(self, *args, **kwargs):
            return self._parent._patch(*args, **kwargs)

        def object(self, *args, **kwargs):
            return self._parent._patch_object(*args, **kwargs)

    class _Mocker:
        def __init__(self):
            self.patch = _PatchHelper(self)

        def patch(self, *args, **kwargs):
            return self._patch(*args, **kwargs)

        def patch_object(self, *args, **kwargs):
            return self._patch_object(*args, **kwargs)

        def _patch(self, *args, **kwargs):
            p = mock.patch(*args, **kwargs)
            target = p.start()
            active_patches.append(p)
            return target

        def _patch_object(self, *args, **kwargs):
            p = mock.patch.object(*args, **kwargs)
            target = p.start()
            active_patches.append(p)
            return target

        def spy(self, obj, name):
            original = getattr(obj, name)
            spy_obj = mock.Mock(wraps=original)
            setattr(obj, name, spy_obj)
            return spy_obj

    def fin():
        for p in reversed(active_patches):
            p.stop()

    request.addfinalizer(fin)
    return _Mocker()
