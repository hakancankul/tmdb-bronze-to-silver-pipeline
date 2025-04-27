cd /dataops
python3 -m venv airflowenv
source airflowenv/bin/activate

pip install --upgrade pip
pip install jupyter
pip install findspark
pip install delta
pip install delta-spark
pip install pandas
pip install apache-airflow-providers-ssh
pip install apache-airflow-providers-bash
pip install 'SQLAlchemy<2.1.0,>=2.0.0'

useradd -rm -d /home/ssh_train -s /bin/bash -g root -G sudo -u 1000 ssh_train
echo 'ssh_train:Ankara06' | chpasswd
chown -R ssh_train /opt/spark/history
su ssh_train -c 'chmod -R 777 /opt/spark/history'

service ssh start

cd /
git clone https://github.com/erkansirin78/data-generator.git
cd data-generator/
python3 -m pip install virtualenv
python3 -m virtualenv datagen

source datagen/bin/activate
pip install -r requirements.txt

sleep infinity