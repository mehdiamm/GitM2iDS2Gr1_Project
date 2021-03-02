from flask import Flask
#from flask_restful import Resource, Api, reqparse, abort, marshal, fields

import os.path
import time

import pandas as pd
import numpy as np

from hdfs import InsecureClient
from hdfs.util import HdfsError

# !pip install cassandra-driver
from cassandra.cluster import Cluster

# Initialize Flask
app=Flask(__name__)

# Variables globales
isRunning = False
stopRunning = False
currentFileToImport = ""



def create_database(session,db,columnFamilyName, replicationFactor = 2, force_replace=False):
	# Suppression du keyspace (base de données) s'il existe et que c'est demandé 
	if force_replace:
		session.execute('DROP KEYSPACE IF EXISTS ' + db)
	
	# Création de la base de données (KEYSPACE)
	request_create_ks = "CREATE KEYSPACE IF NOT EXISTS " + db \
		+ " WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': " + str(replicationFactor) + "}"
	print(request_create_ks)
	session.execute(request_create_ks)

	# Sélection de la base de données
	session.execute('use ' + db)

	# Suppression de la table si elle existe et si demandé
	if force_replace:
		session.execute('DROP TABLE IF EXISTS ' + columnFamilyName)  

	# Création de la table
	request_create_table = "CREATE TABLE IF NOT EXISTS " + columnFamilyName + " ("
	request_create_table += " datetime_ending_ept  TIMESTAMP  "
	request_create_table += " , load_area VARCHAR  "
	request_create_table += " , estimated_load FLOAT  "
	# Ajout de la clé primaire
	request_create_table += ", PRIMARY KEY (datetime_ending_ept)"
	request_create_table += ") ;"
	
	print(request_create_table)

	# Création de la table 
	session.execute(request_create_table)

	# Création de la table de synthèse par jour de la charge totale 
	request_create_table = "CREATE TABLE IF NOT EXISTS " + columnFamilyName + "_summary ("
	request_create_table += " datetime_est_load  TIMESTAMP  "
	request_create_table += " , date_est_load DATE   "
	request_create_table += " , annee INT   "
	request_create_table += " , mois INT   "
	request_create_table += " , semaine INT   "
	request_create_table += " , heure INT   "
	request_create_table += " , trimestre INT   "
	request_create_table += " , jour_annee INT   "
	request_create_table += " , jour_semaine INT   "
	request_create_table += " , jour_mois INT   "
	request_create_table += " , total_estimated_load FLOAT  "
	# Ajout de la clé primaire
	request_create_table += ", PRIMARY KEY (datetime_est_load)"
	request_create_table += ") ;"
	
	print(request_create_table)

	session.execute(request_create_table)




@app.route('/')
def home():
	srvStatus = "<font style='color:red;'> Stopped </font>&nbsp;&nbsp;<a href='http://localhost:5001/start' target='_blank'>Start</a>"
	if isRunning :
		srvStatus = "<font style='color:green;'> Running </font>&nbsp;&nbsp;<a href='http://localhost:5001/stop' target='_blank'>Stop</a><br>Fichier en cours de traitement : " + currentFileToImport + "<br>"
		
	return("""<h1>PJM Data Cleaning API</h1>
		<br><br>Utiliser l'url <a href='http://localhost:5001/start' target='_blank'>localhost:5001/start</a> pour lancer le la récupération des données
		<br>
		<br>
		Statut du service : """ + srvStatus + """
		<br>
		<br>
	""")



@app.route('/start')
def start_service():
	global isRunning
	global stopRunning
	global currentFileToImport
	
	stopRunning = False
	isRunning = True
	
	while not stopRunning:
	
		# Connexion au cluster Cassandra
		cluster = Cluster(contact_points=['cassandra'],port=9042)
		session = cluster.connect()
		session.default_timeout = 10

		# Création de la base de données si elle n'existe pas 
		replicationFactor = 2
		forceReplace = False
		db_name = "pjm"
		columnFamilyName = "estimated_load_hourly"
		create_database(session,db_name,columnFamilyName,replicationFactor,forceReplace)
	
		try:
			# Chemin de téléchargement du fichier des données
			hdfs_file_path = "/user/root/data/pjm"
			
			# Connexion au client HDFS
			client = InsecureClient(url='http://namenode:9870', user='root')

			# Création du dossier de stockage des fichiers traités
			client.makedirs(hdfs_file_path + '/imported')
			
			# Récupération de la liste des fichiers à traiter
			files = client.list(hdfs_file_path ,status=True)
			#print(files)
			#print(client.parts(hdfs_file_path))
			
			# Traitement des fichiers
			for pjm_file, filestatus in files:
				# Mise à jour du nom du fichier pour le status
				currentFileToImport = pjm_file
				print(pjm_file)
				
				
				if filestatus['type'] == 'FILE':
					# Lecture en mémoire du fichier
					with client.read(hdfs_file_path + "/" + pjm_file, encoding='utf-8') as reader:
						df = pd.read_csv(reader,sep=',',header='infer')

						# ----- Transformation initiale du Dataframe -----
						# Suppression des colonnes inutiles
						df = df.drop(columns=['datetime_beginning_ept','datetime_beginning_utc','datetime_ending_utc'])		

						# Suppression des observations en doublon => conservation de la 1e occurence
						df = df.drop_duplicates(subset=['datetime_ending_ept','load_area'],keep='first')

						# Formattage de la date de l'observation
						df['datetime_measure'] = df.apply(lambda x : pd.to_datetime(x[['datetime_ending_ept']], format='%m/%d/%Y %I:%M:%S %p'), axis=1)
						df['datetime_measure'] = df['datetime_measure'].astype('datetime64[ns]')
						df = df.drop(columns=['datetime_ending_ept'])

						# Import des données brutes dans Cassandra (table "estimated_load_hourly" )

						#for i in df.index:
							#request_insert = "INSERT INTO " + columnFamilyName + " (datetime_ending_ept, load_area, estimated_load) " \
							#	+ " VALUES ('" + str(df['datetime_measure'][i]) + "','" + df['load_area'][i] + "', " + str(df['estimated_load_hourly'][i]) + ");" 
							#print(request_insert)
							#session.execute(request_insert)
				
						# ----- Nettoyage des données ---------
						# Objectif : Conservation du total de la consommation par heure 
						
						
						# Suppression de la colonne de la zone  
						df = df.drop(columns=['load_area'])
						
						# Calcul du total de consommation par heure
						df = df.groupby(by=['datetime_measure']).sum()

						# Ajout de colonnes complémentaires à la date de la mesure
						df['date'] = df.index.date
						df['annee'] = df.index.year
						df['mois'] = df.index.month
						df['semaine'] = df.index.isocalendar().week
						df['heure'] = df.index.hour
						df['jour_annee'] = df.index.dayofyear
						df['trimestre'] = df.index.quarter
						df['jour_semaine'] = df.index.dayofweek
						df['jour_mois'] = df.index.day
						
						# Ajout des très rares heures manquantes pour respecter la fréquence des observations
						checkmonth=pd.DataFrame(df.groupby(['jour_annee','annee'])['annee'].count())
						checkmonth.rename(columns={'annee':'nb'}, inplace=True)
						df_missing=checkmonth[checkmonth.nb!=24]
						df_missing=df_missing.reset_index()
						
						Hour0_24=pd.DataFrame(np.arange(24))
						df_to_append=pd.DataFrame()
						
						for x,y in zip(df_missing['jour_annee'],df_missing['annee']):
						    print("Jour avec des heures manquantes :",x,y)
						    
						    df_encours=df[(df.jour_annee==x)&(df.annee==y)]
						    h_missing=Hour0_24[~Hour0_24[0].isin(df_encours.heure)]
						    
						    h=h_missing.iloc[0].name
						    df_to_append=df_to_append.append(df[(df.jour_annee==x)&(df.annee==y)&(df.heure==(h+1))])
							
						df_to_append.heure=df_to_append.heure-1
						df = df.append(df_to_append)
						
						# Insertion des données nettoyées dans CASSANDRA (table "estimated_load_hourly_summary" )
						for label,row in df.iterrows():
							print(label)
							request_insert = "INSERT INTO " + columnFamilyName + "_summary " \
								+ " (datetime_est_load " \
								+ " ,date_est_load " \
								+ " ,annee " \
								+ " ,mois " \
								+ " ,semaine " \
								+ " ,heure " \
								+ " ,trimestre " \
								+ " ,jour_annee " \
								+ " ,jour_semaine " \
								+ " ,jour_mois " \
								+ " ,total_estimated_load " \
								+ " ) " \
								+ " VALUES ('" + str(label) + "' " \
								+ " ,'" + str(row['date']) + "' " \
								+ " ," + str(row['annee']) + " " \
								+ " ," + str(row['mois']) + " " \
								+ " ," + str(row['semaine']) + " " \
								+ " ," + str(row['heure']) + " " \
								+ " ," + str(row['trimestre']) + " " \
								+ " ," + str(row['jour_annee']) + " " \
								+ " ," + str(row['jour_semaine']) + " " \
								+ " ," + str(row['jour_mois']) + " " \
								+ " ," + str(row['estimated_load_hourly']) + " );" 

							print(request_insert)
							session.execute(request_insert)
						
					
					# Déplacement du fichier traités dans le dossier "imported"
					client.rename(hdfs_file_path + "/" + pjm_file,hdfs_file_path +'/imported/' + pjm_file)
					
		except HdfsError as ex:
			# Traitement des erreur HDFS
			print(str(ex))
			
		
		# Fin des fichiers à traiter
		currentFileToImport = ""
		
		#closing Cassandra connection
		session.shutdown()
		
		# Attendre avant de vérifier la présence d'autres fichiers
		time.sleep(30)

	return ""

@app.route('/stop')
def stop_service():
	global stopRunning
	stopRunning = True
	return "Service has been stopped"


if __name__ == "__main__":
    isRunning = False
    currentFileToImport = ""
    app.run(host="0.0.0.0", port=5001,debug=True)
