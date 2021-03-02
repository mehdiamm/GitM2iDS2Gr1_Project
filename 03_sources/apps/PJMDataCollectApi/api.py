from flask import Flask
#from flask_restful import Resource, Api, reqparse, abort, marshal, fields

import os.path
import time

# Utilisation de Requests et Beautiful Soup impossible car la page utilise des composants Javascript

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from hdfs import InsecureClient

# Initialize Flask
app=Flask(__name__)



@app.route('/')
def home():
	return("<h1>PJM Data Collect API</h1><br><br>Utiliser l'url <a href='http://localhost:5000/start'>localhost:5000/start</a> pour lancer le la récupération des données")



@app.route('/start')
def start_service():

	# Chemin de téléchargement du fichier des données
	file_path = "/home/formation/Downloads/hrl_load_estimated.csv"
	
	#Connexion au client HDFS
	client = InsecureClient(url='http://namenode:9870', user='root')

	# Création du dossier /user/root  (non présent par défaut dans l'image docker Hadoop)
	client.makedirs('/user/root')

	# Création d'un dossier data dans le dossier de l'utilisateur root pour y importer les fichiers
	client.makedirs('data')	
	client.makedirs('data/pjm')	

	# Pour chaque année à télécharger
	for year in range(2004,2021):

	    
		# URL de la page de téléchargement des données PJM
		url_pjm_dataminer ="http://dataminer2.pjm.com/feed/hrl_load_estimated"
		#"http://dataminer2.pjm.com/feed/hrl_load_estimated"
		#"http://dataminer2.pjm.com/feed/hrl_load_estimated/definition"


		# Ouverture de Firefox
		# driver = webdriver.Firefox() # pb confirmation téléchargements 
		driver = webdriver.Chrome()
		# Ouverture de la page de téléchargement des données
		driver.get(url_pjm_dataminer)


		# Suppression d'un éventuel ancien fichier téléchargé encore présent dans le dossier de téléchargement
		if os.path.exists(file_path):
			os.remove(file_path)


		try:
			# Attente du chargement du formulaire => champs à renseigner présents
			element = WebDriverWait(driver, 10).until(
			    EC.presence_of_element_located((By.XPATH, "//input[@default-value='defaultStartDate']"))
			)

			#print(driver.page_source)

			# Saisie des dates de début et de fin
			elem =driver.find_element_by_xpath("//input[@default-value='defaultStartDate']")
			elem.clear()
			elem.send_keys("01/01/" +str(year))
			elem =driver.find_element_by_xpath("//input[@default-value='defaultEndDate']")
			elem.clear()
			elem.send_keys("12/31/" +str(year))

			# Attente de rechargement de la page
			time.sleep(5)

			element = WebDriverWait(driver, 10).until(
			    EC.presence_of_element_located((By.XPATH, "//button[text()='Submit']"))
			)

			# Envoi du formulaire pour le rechargement des données pour la période demandée
			elem =driver.find_element_by_xpath("//button[text()='Submit']")
			elem.click()

			# Attente de l'afffichage du bouton d'export
			element = WebDriverWait(driver, 10).until(
			    EC.presence_of_element_located((By.CLASS_NAME, "dm-download"))
			)
			elem =driver.find_element_by_class_name("dm-download")
			elem.click()

			# Attendre le téléchargement du fichier
			while not os.path.exists(file_path):
				time.sleep(1)

			if os.path.isfile(file_path):
				print("Fichier téléchargé pour l'année {}".format(year))

				# Renommage du fichier
				new_file_name = file_path.replace(".csv","_"+str(year)+".csv")
				os.rename(file_path,new_file_name)

				# Upload du fichier CSV local dans le système HDFS
				try:
					remote_load_path = client.upload('/user/root/data/pjm', new_file_name,overwrite=True)
					# print(remote_load_path)
				except:
					print("error")

				print(client.list('/user/root/data/pjm'))

				    
			else:
				raise ValueError("%s isn't a file!" % file_path)
			    
		finally:
			#driver.quit()
			print("fin du traitement du fichier")



		#assert "No results found." not in driver.page_source
		driver.close()

		time.sleep(10)



if __name__ == "__main__":
    app.run(host="0.0.0.0",debug=True)
