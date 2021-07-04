echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo apt-key add
sudo apt-get update -y
sudo apt-get install sbt -y
pip3 install py4j==0.10.9
pip3 install pyspark==3.0.1
pip3 install pytest==5.3.1
cd ~/Desktop/Project/wings-spark-vaccination
rm -rf parser.py
