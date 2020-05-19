from setuptools import setup

setup(
    name='workbot',
    version='0.1.0',
    packages=['workbot'],
    url='https://github.com/kjsanger/workbot',
    license='GPL3',
    author='Keith James',
    author_email='kdj@sanger.ac.uk',
    description='Automation for processing DNA sequence data',
    install_requires=[
        'sqlalchemy>=1.3',
    ],
    tests_require=[
        'pytest',
        'pytest-it'
    ]
)
