from setuptools import setup, find_packages

setup(
    name='DataScienceToolBox',
    version='0.1.0',
    author='Maxen Lee',
    author_email='M.Lee@maxime.group',
    packages=find_packages(),
    url='https://github.com/maxenlee/DataScienceToolBox',
    license='LICENSE.txt',
    description='An awesome data science toolbox.',
    long_description=open('README.md').read(),
    install_requires=[
      'numpy >= 1.11.1'
        # List your project dependencies here.
        # For example: 'numpy >= 1.11.1',
    ],
)
