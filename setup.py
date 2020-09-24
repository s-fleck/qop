from setuptools import setup

setup(
    name='qop',
    version='0.0.1',
    description='Queued File Operations',
    url='http://github.com/s-fleck/qop',
    author='Stefan Fleck',
    author_email='stefan.b.fleck@gmail.com',
    license='MIT',
    packages=['qop'],
    install_requires=['pydub', 'colorama', 'appdirs', 'mutagen'],
    zip_safe=False
)