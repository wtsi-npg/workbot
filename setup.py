from setuptools import setup

setup(name="workbot",
      packages=["workbot"],
      url="https://github.com/wtsi-npg/workbot",
      license="GPL3",
      author="Keith James",
      author_email="kdj@sanger.ac.uk",
      description="Automation for processing DNA sequence data",
      use_scm_version=True,
      python_requires=">=3.8",
      setup_requires=[
              "setuptools_scm"
      ],
      install_requires=[
              "sqlalchemy>=1.3",
      ],
      tests_require=[
              "pytest",
              "pytest-it"
      ],
      scripts=[
              "bin/workbot-init",
              "bin/workbot-add",
              "bin/workbot-run"
      ])
