from setuptools import setup, find_packages

setup(
    name="wherobots-python-dbapi",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        'SQLAlchemy',
    ],
    entry_points='''
            [console_scripts]
            wherobots-python-dbapi=wherobots.db:main
    ''',
)

