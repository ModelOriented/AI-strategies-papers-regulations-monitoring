mkdir /raid/shared/mair
cd /raid/shared/mair
git clone git@github.com:ModelOriented/AI-strategies-papers-regulations-monitoring.git

wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh

conda create -n mair -c rapidsai -c nvidia -c conda-forge cuml=22.04 python=3.8 cudatoolkit=11.0 -y

cd AI-strategies-papers-regulations-monitoring

pip install .

