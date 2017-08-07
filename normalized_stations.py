# normalized_stations.py

stations_to_lines = [
					{ "A" : ["Plaza de Mayo", "Peru", "Piedras", "Lima", "Saenz Pena", "Congreso", "Pasco", "Alberti", "Plaza Miserere", "Loria", "Castro Barros", "Rio de Janeiro", "Acoyte", "Primera Junta", "Puan", "Carabobo", "San Jose de Flores", "San Pedrito"] },

					{ "B" : ["Leandro N. Alem", "Florida", "Carlos Pellegrini", "Uruguay", "Callao", "Pasteur Amia", "Pueyrredon", "Carlos Gardel", "Medrano", "Angel Gallardo", "Malabia O. Pugliese", "Dorrego", "F. Lacroze", "Tronador Villa Ortuzar", "De los Incas Parque Chas", "Echeverria", "Juan Manuel de Rosas Villa Urquiza"] },

					{ "C" : ["Retiro", "General San Martin", "Lavalle", "Diagonal Norte", "Av. de Mayo", "Moreno", "Independencia", "San Juan", "Constitucion"] },

					{ "D" : ["Catedral", "9 de Julio", "Tribunales Teatro Colon", "Callao", "Facultad de Medicina", "Pueyrredon", "Aguero", "Bulnes", "Scalabrini Ortiz", "Plaza Italia", "Palermo", "Ministro Carranza", "Olleros", "Jose Hernandez", "Juramento", "Congreso de Tucuman"] },

					{ "E" : ["Bolivar", "Belgrano", "Independencia", "San Jose", "Entre Rios Rodolfo Walsh", "Pichincha", "Jujuy", "Gral. Urquiza", "Boedo", "Av. La Plata", "Jose Maria Moreno", "Emilio Mitre", "Medalla Milagrosa", "Varela", "Plaza de los Virreyes Eva Peron"] },

					{ "H" : ["Las Heras", "Santa Fe Carlos Jauregui", "Cordoba", "Corrientes", "Once - 30 de Diciembre", "Venezuela", "Humberto I", "Inclan", "Caseros", "Parque Patricios", "Hospitales"] },

 					{ "P" : ["Int. Saguier", "Balbastro", "Mariano Acosta", "Somellera", "Ana Maria Janer", "Ntra. Sra. de Fatima", "Fernandez de la Cruz", "Pte. Illia", "Parque de la Ciudad", "Cecilia Grierson", "Escalada", "Pola", "Ana Diaz", "Centro Civico Lugano", "Larrazabal", "Nicolas Descalzi", "Gabino Ezeiza", "General Savio"] }

]


def stations_by_line(line_letter):
	stations_in_a_line = []
	for dic_line in stations_to_lines:
		if dic_line.get(line_letter):
			stations_in_a_line = dic_line.get(line_letter)
	
	if stations_in_a_line != []:
		return stations_in_a_line
	else:
		print "wrong line letter selected : please choose A, B, C, D, E, H or P"

def all_stations():
	all_stations = []
	for dic_line in stations_to_lines:
		for lista in dic_line.values():
			all_stations = all_stations + lista
	return all_stations
