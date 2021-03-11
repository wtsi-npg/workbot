#!/bin/bash

set -e -u -x

wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-4.6.14-Linux-x86_64.sh -O ~/miniconda.sh

/bin/bash ~/miniconda.sh -b -p ~/miniconda
~/miniconda/bin/conda clean -tipsy
echo ". ~/miniconda/etc/profile.d/conda.sh" >> ~/.bashrc
echo "conda activate base" >> ~/.bashrc

. ~/miniconda/etc/profile.d/conda.sh
conda activate base
conda config --set auto_update_conda False
conda config --add channels "$WSI_CONDA_CHANNEL"
conda config --append channels conda-forge

conda create -y -n travis
conda activate travis
conda install -y baton"$BATON_VERSION"
conda install -y irods-icommands"$IRODS_VERSION"

conda install -y -c defaults --file requirements.txt

grep -v pytest-it test-requirements.txt |\
 xargs -n 1 -I {} conda install -y -c defaults {}
pip install pytest-it==0.1.4

mkdir -p ~/.irods
cat <<EOF > ~/.irods/irods_environment.json
{
    "irods_host": "localhost",
    "irods_port": 1247,
    "irods_user_name": "irods",
    "irods_zone_name": "testZone",
    "irods_home": "/testZone/home/irods",
    "irods_default_resource": "replResc"
}
EOF
