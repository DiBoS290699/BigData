echo 'export PATH=$PATH:/opt/mapr/spark/spark-2.4.5/bin' > /root/.bash_profile
source /root/.bash_profile
hadoop fs -put /mnt/data /Data
hadoop fs -ls /data