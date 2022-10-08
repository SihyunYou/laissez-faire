# © Copyright 2022 La Nouvelle Finance Inc. All rights reserved.
# Bienvenue ! Moi je suis P.D.G. de La Nouvelle Finance et developpeur de cet enfant.  
# Ce script est dependant des politiques de API de UPBIT.
# Attention ! Si vous lisez ce script maintenant, ca veut dire qu'il est experimental, pas operationnel.
# Pourtant, il y a un cle pour que tu puisses pratiquement l'utiliser. Mais ca peut te demander pas mal de temps pour le chercher.
# Bien entendu, je connais le cle. Et vous avez besoin de conclure un contrat avec moi pour user de mon code. 
# N'utilisez pas le source code experimental sans permission du developpeur. Ca pourra merder ton compte.  
# Si vous avez des questions, adressez-vous a caesar2937@gmail.com

import requests
import json
import math
import time
import os
import jwt
import uuid
import hashlib
from urllib.parse import urlencode, unquote
import winsound
import argparse
import numpy as np
import threading
from tqdm import tqdm
import datetime
from datetime import datetime, timedelta
from colorama import init, Fore, Back, Style
import traceback
from enum import Enum, IntEnum
from typing import Final

init(autoreset = True)

UNIT = 3
TEMPS_DORMIR = 0.17
TEMPS_EXCEPTION = 0.25
URL_CANDLE = "https://api.upbit.com/v1/candles/minutes/" + str(UNIT)
CLE_ACCES = ''
CLE_SECRET = ''
URL_SERVEUR = 'https://api.upbit.com'
Sp = 0
uuid_achat = []
uuid_vente = ''
connexion_active = False
Commission = 0.9995

class Niveau:
	INFORMATION = Fore.GREEN + Style.BRIGHT
	SUCCES = Fore.LIGHTWHITE_EX + Back.LIGHTCYAN_EX + Style.BRIGHT
	AVERTISSEMENT = Fore.LIGHTWHITE_EX + Back.LIGHTMAGENTA_EX + Style.BRIGHT
	EXCEPTION = Fore.LIGHTYELLOW_EX + Style.BRIGHT
	ERREUR = Fore.LIGHTWHITE_EX + Back.LIGHTRED_EX + Style.BRIGHT


if not os.path.exists('log'):
	os.makedirs('log')
NOM_FICHE_LOG = 'log\\' + datetime.now().strftime('%m.%d.%X').replace(':', '') + '.txt'

def imprimer(_niveau, _s):
	niveau_datetime = Fore.MAGENTA + Style.NORMAL
	t = '[' + datetime.now().strftime('%m/%d %X') + '] '
	print(niveau_datetime + t + _niveau + _s)
	
	with open(NOM_FICHE_LOG, 'a') as f:
		f.write(t + _s + '\n')

def logger_masse(_n):
	if connexion_active != True:
		return
	try:
		with open("log/masse.txt", 'w') as f:
			global Sp
			f.write(str(int(_n)) + ',' + str(int(Sp)))
	except PermissionError:
		pass

class LOG_ETAT(IntEnum):
	ERREUR = 0
	ATTENDRE = 1
	INITIALISER = 2
	MONITORER = 3
	ACHETER = 4
	INVESTIR = 5
	HORS_DU_TEMPS = 6
	ACHEVER = 7

def logger_etat(_n, _s = ''):
	if connexion_active != True:
		return

	try:
		with open("log/etat.txt", 'w') as f:
			f.write('#' + str(int(_n)))
			if _s != '':
				f.write(',' + _s)
	except PermissionError:
		pass

def tailler(_prix, _taux):
	t = _prix - (_prix / 100) * _taux
	if t < 0.1:
		t = round(t, 4)
	elif t < 1:
		t = round(t, 3)
	elif t < 10: 
		t = round(t, 2)
	elif t < 100:
		t = round(t, 1)
	elif t < 1000:
		t = round(t, 0)
	elif t < 10000:
		t = round(t, 0)
		t -= t % 5
	elif t < 100000:
		t = round(t, 0)
		t -= t % 10
	elif t < 500000:
		t = round(t, 0)
		t -= t % 50
	elif t < 1000000:
		t = round(t, 0)
		t -= t % 100
	elif t < 2000000:
		t = round(t, 0)
		t -= t % 500
	elif 2000000 <= t:
		t = round(t, 0)
		t -= t % 1000

	return t

def coller(_prix):
	t = _prix
	if t < 0.1:
		t += 0.0001
	elif t < 1:
		t += 0.001
	elif t < 10: 
		t += 0.01
	elif t < 100:
		t += 0.1
	elif t < 1000:
		t += 1
	elif t < 10000:
		t += 5
	elif t < 100000:
		t += 10
	elif t < 500000:
		t += 50
	elif t < 1000000:
		t += 100
	elif t < 2000000:
		t += 500
	elif 2000000 <= t:
		t += 1000

	return t


class RecupererCodeMarche:
	def __init__(self):
		try:
			headers = {"Accept": "application/json"}
			response = requests.request("GET", "https://api.upbit.com/v1/market/all?isDetails=false", headers=headers)
			self.dict_response = json.loads(response.text)
		except:
			imprimer(Niveau.ERREUR, "Rate de recuperer la liste de symbols. (1)")
			raise Exception('Recuperer code de marche')


class RecupererInfoCandle:
	def __recuperer_array(self, _s : str, _n : int):
		arr = np.zeros(_n)
		for i in range(_n):
			arr[_n - i - 1] = self.dict_response[i].get(_s)
		return arr

	def __init__(self, _symbol : str):
		comte = 200
		querystring = {
			"market" : "KRW-" + _symbol,
			"count" : str(comte)
		}

		try:
			response = requests.request("GET", URL_CANDLE, params=querystring)
			self.dict_response = json.loads(response.text)

			time.sleep(0.052)
		except:
			imprimer(Niveau.EXCEPTION, "Rate de recuperer les donnes de prix.")
			raise Exception("RecupererInfoCandle")

		self.array_opening_price = self.__recuperer_array('opening_price', comte)
		self.array_trade_price = self.__recuperer_array('trade_price', comte)
		self.prix_courant = self.array_trade_price[-1]
		self.array_high_price = self.__recuperer_array('high_price', comte)
		self.array_low_price = self.__recuperer_array('low_price', comte)
		self.array_acc_trade_price = self.__recuperer_array('candle_acc_trade_price', comte)


class Verifier:
	def __init__(self, _symbol, _n = 20):
		self.n = _n
		self.candle = RecupererInfoCandle(_symbol)
		self.ecart_type = np.std(np.array(self.candle.array_trade_price)[-1 * _n : -1])
		self.ecart_type_regularise = self.ecart_type / self.candle.prix_courant
		self.msm = np.mean(np.array(self.candle.array_trade_price)[-1 * _n : -1])

	##### Premiere verification #####
	def verfier_surete(self):
		p = self.candle.array_trade_price[-60]
		q = self.candle.prix_courant
		if - 0.2 < (q - p) / self.candle.prix_courant < 0.4 and \
			self.candle.prix_courant < self.msm - self.ecart_type * 0: # 0.2533(10%), 0.5243(20%)
			return True
		return False

	def verifier_prix(self):
		if 0.03 < self.candle.prix_courant < 0.1 or 0.3 < self.candle.prix_courant < 1 or \
			3 < self.candle.prix_courant < 10 or 30 < self.candle.prix_courant < 100 or \
			300 < self.candle.prix_courant < 1000 or 1800 < self.candle.prix_courant:
			return True
		return False


	##### Deuxieme verification #####
	def verifier_bb_variable(self):
		# x = std_regularise(RSD), y = z-note(RID)
		# y <= 144x - 2.72

		z = (self.candle.prix_courant - self.msm) / self.ecart_type 
		if self.ecart_type_regularise >= 0.003 and z <= 0:
			if z <= 144 * self.ecart_type_regularise - 2.72:
				self.z = z
				imprimer(Niveau.INFORMATION, 
							"Hors de bb_variable ! z : " + str(round(z, 3)) + 
							", std_regularise : " + str(round(self.ecart_type_regularise, 5)))
				return True
		return False

	def obtenir_vr(self):
		h, b, e = 0, 0, 0
		for i in range(-1 * self.n, 0):
			p = self.candle.array_trade_price[i] - self.candle.array_opening_price[i]
			if p > 0:
				h += self.candle.array_acc_trade_price[i]
			elif p < 0:
				b += self.candle.array_acc_trade_price[i]
			else:
				e += self.candle.array_acc_trade_price[i]

		if b <= 0 and e <= 0:
			return -1
		else:
			return (h + e * 0.5) / (b + e * 0.5) * 100

	def verifier_vr(self, _p):
		if self.ecart_type_regularise >= 0.005:
			self.vr = self.obtenir_vr()
			if self.vr != -1:
				if self.vr <= _p:
					imprimer(Niveau.INFORMATION, 
								"Hors de vr ! vr : " + str(round(self.vr, 2)))
					return True
		return False

	def verifier_rdivr_integre(self):		
		if self.ecart_type_regularise > 0.003:
			rdi = (self.candle.prix_courant - self.msm) / self.ecart_type
			if rdi <= 144 * self.ecart_type_regularise - 2.72:
				vr = self.obtenir_vr()
				if rdi < 0 and vr < 70:
					self.cnv = abs(rdi - 1) / vr * 500
					if self.cnv > 100:
						imprimer(Niveau.INFORMATION, 
									"Suffire a rdivr_integre ! cnv : " + str(round(self.cnv, 3)) +
									", rdi : " + str(round(rdi, 2)) +
									", vr : " + str(round(vr, 2)))
						return True
		return False


class Annuler:
	def annuler_achats(self):
		global uuid_achat

		while True:
			try:
				for p in uuid_achat:
					self.annuler_commande(p)
				uuid_achat.clear()
				return
			except:
				imprimer(Niveau.EXCEPTION, "Rate d'annuler le demande d'achat.")
				time.sleep(TEMPS_EXCEPTION)
			
	def annuler_vente(self):
		global uuid_vente
		
		while True:
			try:
				self.annuler_commande(uuid_vente)
				uuid_vente = ''
				return
			except:
				imprimer(Niveau.EXCEPTION, "Rate d'annuler le demande de vente.")
				time.sleep(TEMPS_EXCEPTION)

	# @ _type
	ACHAT: Final = 1
	VENTE: Final = 2
	TOUT: Final = 3
	def annuler_precommandes(self, _type = ACHAT):
		params = {
			'state': 'wait'
		}
		query_string = unquote(urlencode(params, doseq=True)).encode("utf-8")

		m = hashlib.sha512()
		m.update(query_string)
		query_hash = m.hexdigest()

		payload = {
			'access_key': CLE_ACCES,
			'nonce': str(uuid.uuid4()),
			'query_hash': query_hash,
			'query_hash_alg': 'SHA512',
		}

		jwt_token = jwt.encode(payload, CLE_SECRET)
		authorization = 'Bearer {}'.format(jwt_token)
		headers = {
		  'Authorization': authorization,
		}

		response = requests.get(URL_SERVEUR + '/v1/orders', params=params, headers=headers)
		dict_response = json.loads(response.text)
		#print(dict_response)
			
		time.sleep(TEMPS_DORMIR)

		if 1 == _type:
			for mon_dict in dict_response:
				if mon_dict.get('side') == 'bid':
					self.annuler_commande(mon_dict.get('uuid'))
		elif 2 == _type:
			for mon_dict in dict_response:
				if mon_dict.get('side') == 'ask':
					self.annuler_commande(mon_dict.get('uuid'))
		elif 3 == _type:
			for mon_dict in dict_response:
				self.annuler_commande(mon_dict.get('uuid'))

	def annuler_commande(self, _uuid):
		global CLE_ACCES
		params = {
			'uuid': _uuid,
		}
		query_string = unquote(urlencode(params, doseq=True)).encode("utf-8")

		m = hashlib.sha512()
		m.update(query_string)
		query_hash = m.hexdigest()

		payload = {
			'access_key': CLE_ACCES,
			'nonce': str(uuid.uuid4()),
			'query_hash': query_hash,
			'query_hash_alg': 'SHA512',
		}

		jwt_token = jwt.encode(payload, CLE_SECRET)
		authorize_token = 'Bearer {}'.format(jwt_token)
		headers = {"Authorization": authorize_token}

		response = requests.delete(URL_SERVEUR + "/v1/order", params=params, headers=headers)
		dict_response = json.loads(response.text)
		#print(dict_response)
			
		time.sleep(TEMPS_DORMIR)
		

class ExaminerCompte:
	def __init__(self):
		payload = {
			'access_key': CLE_ACCES,
			'nonce': str(uuid.uuid4()),
		}

		jwt_token = jwt.encode(payload, CLE_SECRET)
		authorize_token = 'Bearer {}'.format(jwt_token)
		headers = {"Authorization": authorize_token}

		while True:
			try:
				response = requests.get(URL_SERVEUR + "/v1/accounts", headers=headers)
				self.dict_response = json.loads(response.text)
				time.sleep(TEMPS_DORMIR)
				break
			except:
				time.sleep(TEMPS_DORMIR)

	# @ _type
	SOLDE: Final = 1
	FERME: Final = 2
	TOUT: Final = 3
	def recuperer_solde_krw(self, _type = SOLDE):
		for mon_dict in self.dict_response:
			if mon_dict.get('currency') == "KRW":
				if 1 == _type:
					return float(mon_dict.get('balance'))
				elif 2 == _type:
					return float(mon_dict.get('locked'))
				elif 3 == _type:
					return float(mon_dict.get('balance')) + float(mon_dict.get('locked'))
		return 0

	def recuperer_symbols(self):
		symbols = []
		for mon_dict in self.dict_response:
			symbols.append(mon_dict.get('currency'))
		return symbols

	def recuperer_symbol_info(self, _symbol):
		for mon_dict in self.dict_response:
			if mon_dict.get('currency') == _symbol:
				solde = float(mon_dict.get('balance'))
				ferme = float(mon_dict.get('locked'))
				prix_moyenne_achat = float(mon_dict.get('avg_buy_price'))

				return solde, ferme, prix_moyenne_achat
		return -1, -1, -1


class Acheter:
	def __init__(self, _symbol, _prix_courant, _somme_totale, _poids):
		self.symbol = _symbol
		self.prix_courant = _prix_courant
		self.S = _somme_totale
		self.poids = _poids

	class Diviser(Enum):
		LINEAIRE = 1
		LOG_LINEAIRE_II = 2
		LOG_LINEAIRE_I = 3
		PARABOLIQUE_II = 4
		PARABOLIQUE_I = 5
		EXPOSANT = 6
		LAPIN = 7

	def diviser_integre(self, _pourcent_descente : float, _fois_decente : int, _facon : int):
		if self.Diviser.LINEAIRE == _facon: # n
			self.diviser_lineaire(_pourcent_descente, _fois_decente, 16777216)
		elif self.Diviser.LOG_LINEAIRE_II == _facon: # n * log(n + 3)
			self.diviser_log_lineaire(_pourcent_descente, _fois_decente, 3)
		elif self.Diviser.LOG_LINEAIRE_I == _facon: # n * log(n + 2)
			self.diviser_log_lineaire(_pourcent_descente, _fois_decente, 2)
		elif self.Diviser.PARABOLIQUE_II == _facon: # 2.5n^2 + 2.5n + 5
			self.diviser_parabolique2(_pourcent_descente, _fois_decente)
		elif self.Diviser.PARABOLIQUE_I == _facon: # n^2 - 0.5n + 1
			self.diviser_parabolique(_pourcent_descente, _fois_decente)
		elif self.Diviser.EXPOSANT == _facon: # 1.2^n
			self.diviser_exposant(_pourcent_descente, _fois_decente, 1.2)
		elif self.Diviser.LAPIN == _facon: # fibonacci varie
			self.diviser_lapin(_pourcent_descente, _fois_decente)

	def diviser_lineaire(self, _pourcent_descente, _fois_decente, _difference):
		r = _fois_decente
		h = _difference
		a = self.S / (r * ((r + 1) * h / 200 + 1))

		for n in range(1, _fois_decente + 1):
			poids_hauteur = 1 + self.poids * (n - 1)
			pn = tailler(coller(self.prix_courant), (n - 1) * (_pourcent_descente * poids_hauteur))
			qn = a * h * n / 100 + a
			self.acheter(pn, qn)

	def diviser_log_lineaire(self, _pourcent_descente, _fois_decente, _poids):
		s = 0
		for n in range(1, _fois_decente + 1):
			s += n * math.log(n + _poids)
		
		for n in range(1, _fois_decente + 1):
			poids_hauteur = 1 + self.poids * (n - 1)
			pn = tailler(coller(self.prix_courant), (n - 1) * (_pourcent_descente * poids_hauteur))
			qn = self.S * (n * math.log(n + _poids)) / s
			self.acheter(pn, qn) 

	def diviser_parabolique(self, _pourcent_descente, _fois_decente): # non-recommande
		s = _fois_decente * (pow(_fois_decente, 2) + 5) / 6
		for n in range(1, _fois_decente + 1):
			poids_hauteur = 1 + self.poids * (n - 1)
			pn = tailler(coller(self.prix_courant), (n - 1) * (_pourcent_descente * poids_hauteur))
			kn = (pow(n, 2) / 2) - (n / 2) + 1
			qn = self.S * kn / s
			self.acheter(pn, qn)

	def diviser_parabolique2(self, _pourcent_descente, _fois_decente):
		s = _fois_decente * (5 * pow(_fois_decente, 2) + 15 * _fois_decente + 40) / 6
		for n in range(1, _fois_decente + 1):
			poids_hauteur = 1 + self.poids * (n - 1)
			pn = tailler(coller(self.prix_courant), (n - 1) * (_pourcent_descente * poids_hauteur))
			kn = 5 / 2 * pow(n, 2) + 5 / 2 * n + 5
			qn = self.S * kn / s
			self.acheter(pn, qn)

	def diviser_exposant(self, _pourcent_descente, _fois_decente, _exposant): # non-recommande
		h = _fois_decente
		r = _exposant
		a = self.S * (r - 1) / (pow(r, h) - 1)

		for n in range(1, _fois_decente + 1):
			poids_hauteur = 1 + self.poids * (n - 1)
			pn = tailler(coller(self.prix_courant), (n - 1) * (_pourcent_descente * poids_hauteur))
			qn = a * pow(r, n - 1)
			self.acheter(pn, qn)

	def diviser_lapin(self, _pourcent_descente, _fois_decente): # non-recommande
		lapin = [1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181, 6765, 10946] # 20
		mon_lapin = lapin[:_fois_decente - 1]

		for n in range(1, _fois_decente + 1):
			poids_hauteur = 1 + self.poids * (n - 1)
			pn = tailler(coller(self.prix_courant), (n - 1) * (_pourcent_descente * poids_hauteur))
			qn = self.S * lapin[n - 1] / sum(mon_lapin)
			self.acheter(pn, qn) 

	def acheter(self, _pn, _qn):
		query = {
			'market': "KRW-" + self.symbol,
			'side': 'bid',
			'volume': str(_qn / _pn), 
			'price': str(_pn),
			'ord_type': 'limit',
		}
		query_string = urlencode(query).encode()

		m = hashlib.sha512()
		m.update(query_string)
		query_hash = m.hexdigest()

		payload = {
			'access_key': CLE_ACCES,
			'nonce': str(uuid.uuid4()),
			'query_hash': query_hash,
			'query_hash_alg': 'SHA512',
		}

		jwt_token = jwt.encode(payload, CLE_SECRET)
		authorize_token = 'Bearer {}'.format(jwt_token)
		headers = {"Authorization": authorize_token}

		response = requests.post(URL_SERVEUR + "/v1/orders", params=query, headers=headers)
		dict_response = json.loads(response.text)
		global uuid_achat
		uuid_achat.append(dict_response.get('uuid'))

		#print(response.text)
		time.sleep(TEMPS_DORMIR)


class Vendre:
	def __init__(self, _symbol : str, _volume : float, _prix : float):
		query = {
			'market': 'KRW-' + _symbol,
			'side': 'ask',
			'volume': _volume,
			'price': _prix,
			'ord_type': 'limit',
		}
		query_string = urlencode(query).encode()

		m = hashlib.sha512()
		m.update(query_string)
		query_hash = m.hexdigest()

		payload = {
			'access_key': CLE_ACCES,
			'nonce': str(uuid.uuid4()),
			'query_hash': query_hash,
			'query_hash_alg': 'SHA512',
		}

		jwt_token = jwt.encode(payload, CLE_SECRET)
		authorize_token = 'Bearer {}'.format(jwt_token)
		headers = {"Authorization": authorize_token}

		response = requests.post(URL_SERVEUR + "/v1/orders", params=query, headers=headers)
		dict_response = json.loads(response.text)
		time.sleep(TEMPS_DORMIR)
		
		global uuid_vente
		uuid_vente = dict_response.get('uuid')


class ControlerVente:
	def __init__(self):
		self.flag_commande_vendre = False
		self.count_montant_insuffissant = 0
		self.t = 0

	def est_commande_vente_complete(self, _symbol : str):
		ec = ExaminerCompte()
		symbols = ec.recuperer_symbols()

		if self.count_montant_insuffissant > 300:
			imprimer(Niveau.AVERTISSEMENT, "Annuler le vente car le reste de demande de vente n'est pas conclu.")
			self.count_montant_insuffissant = 0
			self.flag_commande_vendre = False
			return True

		for symbol in symbols:
			try:
				solde, ferme, prix_moyenne_achat = ec.recuperer_symbol_info(symbol)
				montant = (solde + ferme) * prix_moyenne_achat
			except:
				time.sleep(TEMPS_DORMIR)
				return False

			if symbol == _symbol:
				if montant < 5000:
					self.count_montant_insuffissant += 1
				else:
					self.count_montant_insuffissant = 0

				if solde + ferme > 0.00001:
					return False
				else:
					break

		if self.flag_commande_vendre == False:
			return False
		else:
			self.flag_commande_vendre = False
			return True

	def vendre_a_plein(self, _symbol : str, _proportion_profit : float):
		try:
			# solde, ferme <- volume
			solde, ferme, prix_moyenne_achat = ExaminerCompte().recuperer_symbol_info(_symbol)
			if solde < 0:
				return False
			
			if connexion_active:
				if self.t % 10 == 0:	
					masse_realisee = ExaminerCompte().recuperer_solde_krw(ExaminerCompte.TOUT)
					masse_irrealisee = (solde + ferme) * RecupererInfoCandle(_symbol).prix_courant * Commission
					logger_masse(int(masse_realisee + masse_irrealisee))
				self.t += 1

			montant = (solde + ferme) * prix_moyenne_achat
			self.count_montant_insuffissant = 0

			# Yay, ca pourra reperer la derniere erreur !
			if solde > 1000 / prix_moyenne_achat and montant > 5000: 
				if uuid_vente != '':
					Annuler().annuler_vente()
					time.sleep(TEMPS_DORMIR)
				time.sleep(TEMPS_DORMIR)

				solde, ferme, prix_moyenne_achat = ExaminerCompte().recuperer_symbol_info(_symbol)
				imprimer(Niveau.INFORMATION, 
							"prix de moyenne d'achat : " + str(prix_moyenne_achat) + ", position de vente : " + str(tailler(prix_moyenne_achat, -1 * _proportion_profit)))
				Vendre(_symbol, solde + ferme, tailler(prix_moyenne_achat, -1 * _proportion_profit))

				self.flag_commande_vendre = True
				return True
			elif montant >= 5000:
				return True
			else:
				return False	
		except Exception as e:
			traceback.print_exc()
		return False


if __name__=="__main__":
	try:
		with open("key.txt", 'r') as f:
			CLE_ACCES = f.readline().strip()
			CLE_SECRET = f.readline().strip()
			imprimer(Niveau.INFORMATION, "CLE_ACCES : " + CLE_ACCES)

		TEMPS_INITIAL = datetime.now()
		TEMPS_REINITIAL = datetime.now() - timedelta(hours = 24)
		nom_symbol = ''
		idx = 0
		animation = "|/-\\"
		deuxieme_initialisation = False

		Annuler().annuler_precommandes(Annuler.ACHAT)
		Sp = S = ExaminerCompte().recuperer_solde_krw()
		imprimer(Niveau.INFORMATION, "KRW disponible : " + format(int(S), ','))
		logger_masse(S)

		parser = argparse.ArgumentParser(description="T'es vraiment qu'un sale petit.")
		parser.add_argument('-a', type=int, required=False, help="-a : le type d'annulation de precommandes")
		parser.add_argument('-c', type=bool, required=False, help="-c : s'il faut se connecter a WindowsForm (pas pour un cmd)")
		parser.add_argument('-d', type=float, required=False, help="-d : la proportion divise")
		parser.add_argument('-f', type=int, required=False, help="-f : la facon d'achat divise")
		parser.add_argument('-i', type=int, required=False, help="-i : l'intervalle de reinitialisation de liste d'achat (heures)")
		parser.add_argument('-p', type=float, required=False, help="-p : le poids de division")
		parser.add_argument('-s', type=int, required=False, help="-s : la somme totale")
		parser.add_argument('-t', type=int, required=False, help="-t : le temps timeout (seconds)")
		parser.add_argument('-v', type=float, required=False, help="-v : la position de vente")
		args = parser.parse_args()
	
		if args.a is not None:
			__facon_achat = args.a
		else:
			__facon_achat = 1

		if args.c is not None:
			connexion_active = args.c
		else:
			connexion_active = False

		if args.d is not None:
			__proportion_divise = args.d
		else:
			__proportion_divise = 0.333

		if args.f is not None:
			__facon_achat = args.f
		else:
			__facon_achat = Acheter.Diviser.LOG_LINEAIRE_II

		if args.i is not None:
			__intervallle_reinitialisation = args.i
		else:
			__intervallle_reinitialisation = 4

		if args.p is not None:
			__poids_divise = args.p
		else:
			__poids_divise = 0.018
	
		if args.s is not None:
			if args.s < 10000000:
				imprimer(Niveau.ERREUR, "Vous devez saisir plus de 10,000,000 won.")
				exit()
			else:
				S = int(args.s * Commission)
		else:
			S = int(S * Commission)
	
		if args.t is not None:
			__temps_timeout = args.t
		else:
			__temps_timeout = 30

		if args.v is not None:
			__position_vente = args.v
		else:
			__position_vente = 0.32


		while True:
			if datetime.now() - TEMPS_REINITIAL > timedelta(hours = __intervallle_reinitialisation):
				logger_etat(LOG_ETAT.INITIALISER)

				TEMPS_REINITIAL = datetime.now()
				list_symbols, list_symbols_ = [], []

				if deuxieme_initialisation:
					Annuler().annuler_precommandes(Annuler.ACHAT)
				else:
					deuxieme_initialisation = True

				with open("reputation.csv", 'r') as f:
					list_reputations = [line.strip() for line in f]

				for reputation in list_reputations:
					t = reputation.split(',')
					symbol, note, capitalisation = t[0], int(t[1]), float(t[2])

					if note > 80 or note >= 50 and capitalisation * note >= 20:
						# Ca veut dire que la capitalisation est au moins plus que 400 milliards.
						list_symbols_.append(symbol)		

				for symbol in tqdm(list_symbols_, desc = 'Initialisation'):
					r = RecupererInfoCandle(symbol)
					if 0.027 < r.prix_courant < 0.102 or 0.27 < r.prix_courant < 1.02 or \
						2.7 < r.prix_courant < 10.2 or 27 < r.prix_courant < 102 or \
						270 < r.prix_courant < 1020 or 1500 < r.prix_courant:
						list_symbols.append(symbol)

				imprimer(Niveau.INFORMATION, 
							"Monitorer la liste suivie de crypto monnaies qui suffit a la critere d'achat.\n" + \
							'[' + ', '.join(list_symbols) + ']')	

				if nom_symbol != '' and nom_symbol in list_symbols:
					list_symbols.remove(nom_symbol)
					list_symbols.insert(0, nom_symbol)
			else:
				breakable, flag_commande_vendre = False, False
				fault = 0

				while True:
					if breakable: 
						break
				
					logger_etat(LOG_ETAT.MONITORER)
					for symbol in list_symbols:
						if breakable: 
							break
					
						idx += 1
						print("En train de monitorer..." + animation[idx % len(animation)], end="\r")
					
						try:
							v = Verifier(symbol, 20)
							if v.verfier_surete() and v.verifier_prix():
								verification_passable = True
								if v.verifier_rdivr_integre():
									t = 36 - int((v.cnv - 100) / 18)
								elif v.verifier_bb_variable():
									t = 36 + int(v.z * 1.8)
								elif v.verifier_vr(40):
									t = 30 + int(v.vr / 7)
								else:
									verification_passable = False
								
								if verification_passable:
									logger_etat(LOG_ETAT.ACHETER, symbol)
									a = Acheter(symbol, v.candle.prix_courant, S, __poids_divise)
									a.diviser_integre(__proportion_divise, t, __facon_achat)
								
									nom_symbol = symbol
									breakable = True
									imprimer(Niveau.INFORMATION, "Acheve de demander l'achat \'" + symbol + '\'')
									
									if connexion_active != True:
										t = threading.Thread(target = winsound.Beep, args=(440, 500))
										t.start()
						except Exception:
							traceback.print_exc()
			
				if nom_symbol != '' and nom_symbol in list_symbols:
					list_symbols.remove(nom_symbol)
					list_symbols.insert(0, nom_symbol)

				cv = ControlerVente()
				while True:
					if cv.est_commande_vente_complete(nom_symbol):
						logger_etat(LOG_ETAT.ACHEVER)
						imprimer(Niveau.SUCCES, "Vente achevee. Annuler le reste de demandes d'achat.")
						break
					elif fault >= __temps_timeout:
						logger_etat(LOG_ETAT.HORS_DU_TEMPS)
						imprimer(Niveau.AVERTISSEMENT, "Hors du temps.")
						break

					if cv.vendre_a_plein(nom_symbol, __position_vente):
						logger_etat(LOG_ETAT.INVESTIR, nom_symbol)
						fault = 0
					else:
						fault += 1

				Annuler().annuler_achats()
				S = int(ExaminerCompte().recuperer_solde_krw())
				imprimer(Niveau.INFORMATION,
							"Interet : " + '{0:+,}'.format(int(S - Sp)) + ' (' + str(datetime.now() - TEMPS_INITIAL) + ')')
				logger_masse(S)
				S = int(S * Commission)
	except Exception:
		logger_etat(LOG_ETAT.ERREUR)
		traceback.print_exc()
		time.sleep(9999999)