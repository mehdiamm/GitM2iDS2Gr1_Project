from hdfs import InsecureClient


#Connexion au client HDFS
client = InsecureClient(url='http://namenode:9870', user='root')

# Création du dossier /user/root  (non présent par défaut dans l'image docker Hadoop)
client.makedirs('/user/root')

# Création d'un dossier data dans le dossier de l'utilisateur root pour y importer les fichiers
client.makedirs('data')	
client.makedirs('data/pjm')	

file_path = "/home/formation/Downloads/"
file_name = file_path + "test.txt"
remote_load_path = client.upload('/user/root/data/pjm', file_name,overwrite=True)

print(client.list('/user/root/data/pjm'))
