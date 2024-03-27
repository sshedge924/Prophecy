from setuptools import setup, find_packages
packages_to_include = find_packages(exclude = ['test.*', 'test', 'test_manual'])
setup(
    name = 'santoshcloudaeonnetteam_projectgitsantosh',
    version = '0.8',
    packages = packages_to_include,
    description = '',
    install_requires = [
'Faker==24.3.0', 'pandas==2.2.1', 'numpy==1.26.4', 'DateTime==5.5', 'random2==1.0.2', ],
    data_files = ["resources/extensions.idx"]
)
