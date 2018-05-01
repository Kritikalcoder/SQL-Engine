import sys
import re

join_cndns = []
dictionary = {}

def display_res(table):
	print ','.join(table['info'])
	for row in table['table']:
		print ','.join([str(x) for x in row])

def cartesian_prd(table1, table2):
	prod_table = {}
	prod_table['table'] = []
	prod_table['info'] = []
	temp1 = []
	for field in table1['info']:
		if len(field.split('.')) == 1:
			temp1.append(table1['name'] + '.' + field)
		else:
			temp1.append(field)

	temp2 = []
	for field in table2['info']:
		if len(field.split('.')) == 1:
			temp2.append(table2['name'] + '.' + field)
		else:
			temp2.append(field)


	prod_table['info'] = prod_table['info'] + temp1 + temp2

	for row1 in table1['table']:
		for row2 in table2['table']:
			prod_table['table'].append(row1 + row2)

	return prod_table

def select(tables, condition_str):
	
	result_table = {}

	if len(tables) == 1:
		joined_table = cartesian_prd(dictionary[tables[0]], {'info': [], 'table': [[]]})

	else:
		joined_table = cartesian_prd(dictionary[tables[0]], dictionary[tables[1]])

	if len(tables) > 2:
		for i in xrange(len(tables) - 2):
			joined_table = cartesian_prd(joined_table, dictionary[tables[i + 2]])

	result_table['info'] = []
	for x in joined_table['info']:
		result_table['info'].append(x)

	condition_str = re.sub('(?<=[\w ])(=)(?=[\w ])', '==', condition_str)

	conditions = condition_str.replace(" and ", ",")
	conditions = conditions.replace(" or ", ",")
	conditions = conditions.replace('(', '')
	conditions = conditions.replace(')', '')
	conditions = conditions.split(',')

	for condition in conditions:
		if bool(re.match('.*==.*[a-zA-Z]+.*', condition.strip())):
			temp1 = condition.strip()
			temp1 = temp1.split('==')[0]
			temp1 = temp1.strip()

			temp2 = condition.strip()
			temp2 = temp2.split('==')[1]
			temp2 = temp2.strip()
			join_cndn = (temp1, temp2)
			join_cndns.append(join_cndn)

	for field in joined_table['info']:
			condition_str = condition_str.replace(field, 'row[' + str(joined_table['info'].index(field)) + ']')

	result_table['table'] = []

	for row in joined_table['table']:
		if eval(condition_str):
			result_table['table'].append(row)

	return result_table

def project(table, fields, dist_flag, aggr_flag):

	result_table = {}
	result_table['info'] = []
	result_table['table'] = []

	if aggr_flag is not None:
		result_table['info'].append(aggr_flag + "(" + fields[0] + ")")
		field_index = table['info'].index(fields[0])

		temp = []
		for row in table['table']:
			temp.append(row[field_index])

		aggr_dict = {
			'sum':sum(temp),
			'max':max(temp),
			'min':min(temp),
			'avg':(sum(temp) * 1.0)/len(temp)
		}

		result_table['table'].append([aggr_dict[aggr_flag]])

	else:
		if fields[0] == '*':
			temp = []
			for x in table['info']:
				temp.append(x)
			fields[:] = temp[:]

			for field_pair in join_cndns:
				temp[:] = []
				for x in fields:
					if x != field_pair[1]:
						temp.append(x)

				fields[:] = temp[:]

		result_table['info'] += fields
		field_indices = []

		for field in fields:
			ind = table['info'].index(field)
			field_indices.append(ind)

		for row in table['table']:
			result_row = []
			for i in field_indices:
				result_row.append(row[i])
			result_table['table'].append(result_row)

		if dist_flag:
			temp = sorted(result_table['table'])
			result_table['table'][:] = []
			for i in range(len(temp)):
				if i == 0 or temp[i] != temp[i-1]:
					result_table['table'].append(temp[i])	

	return result_table

def semicolon_error(query):
	if query[len(query) - 1] != ';':
		print "Semicolon missing"
		return 1
	return 0

def format_error(query):
	if bool(re.match('^select.*from.*', query)) is False:
		print "Invalid query"
		return 1
	return 0

def aggr_error(aggr_flag, leng):
	if aggr_flag is not None and leng > 1:
		print "Too many arguments"
		return 1
	return 0

def table_error(table):
	if table not in dictionary:
			print "Invalid table - " + table
			return 1
	return 0

def field_error(field_flag):
	if field_flag != 1:
		print "Invalid field"
		return 1
	return 0

def check_field_validity(fields, tables):
	for field in fields:
		field_flag = 0
		for table in tables:
			if field.split('.')[-1] in dictionary[table]['info']:
				if len(field.split('.')) == 2 and field.split('.')[0] == table:
						field_flag = field_flag + 1
				else:
					field_flag = field_flag + 1

		if field_error(field_flag):
			return 0

	return 1

def parse(query):

	dist_flag = False
	aggr_flag = None
	star_flag = False

	if semicolon_error(query):
		return

	query = query.strip(';')
	
	if format_error(query):
		return

	fields = query.split('from')[0]
	fields = fields.replace('select', '')
	fields = fields.strip()

	if bool(re.match('^distinct.*', fields)):
		dist_flag = True
		fields = fields.replace('distinct', '')
		fields = fields.strip()

	if bool(re.match('^(sum|max|min|avg)\(.*\)', fields)):
		aggr_flag = fields.split('(')[0]
		aggr_flag = aggr_flag.strip()
		fields = fields.replace(aggr_flag, '')
		fields = fields.strip()
		fields = fields.strip('()')

	fields = fields.split(',')
	for i in xrange(len(fields)):
		fields[i] = fields[i].strip()

	if len(fields) == 1: 
		if fields[0] == '*':
			star_flag = True

	if aggr_error(aggr_flag, len(fields)):
		return

	tables = query.split('from')[1]
	tables = tables.split('where')[0]
	tables = tables.strip()
	tables = tables.split(',')

	for i in xrange(len(tables)):
		tables[i] = tables[i].strip()

	for table in tables:
		if table_error(table):
			return

	if bool(re.match('^select.*from.*where.*', query)):
		if star_flag is False:
			if check_field_validity(fields, tables) == 0:
				return

		condition_str = query.split('where')[1]
		condition_str = condition_str.strip()
		temp = condition_str.replace(' and ', ' ')
		temp = temp.replace(' or ', ' ')

		cond_cols = re.findall(r"[a-zA-Z][\w\.]*", temp)
		cond_cols = set(cond_cols)
		cond_cols = list(cond_cols)

		if check_field_validity(cond_cols, tables) == 0:
			return

		if star_flag is False:
			for i in xrange(len(fields)):
				if len(fields[i].split('.')) == 1:
					for table in tables:
						appended_name = table + '.'
						if fields[i] not in dictionary[table]['info']:
							continue
						else:
							fields[i] = appended_name + fields[i]
							break

		for field in cond_cols:
			if len(field.split('.')) == 1:
				for table in tables:
					if field not in dictionary[table]['info']:
						continue
					else:
						temp1 = table + '.' + field
						temp2 = ' ' + condition_str
						condition_str = re.sub('(?<=[^a-zA-Z0-9])(' + field + ')(?=[\(\)= ])', temp1, temp2)
						condition_str = condition_str.strip(' ')

		display_res(project(select(tables, condition_str), fields, dist_flag, aggr_flag))

	else:
		if len(tables) >= 2:
			print "More tables than required"
			return

		if star_flag is not True:
			for field in fields:
				if field in dictionary[tables[0]]['info']:
					continue
				else:
					print "Invalid field - " + field
					return

		display_res(project(dictionary[tables[0]], fields, dist_flag, aggr_flag)) 

def read_meta_data():
	with open('./metadata.txt', 'r') as f:
		line = f.readline().strip()
		while line:
			if line == "<begin_table>":
				table_name = f.readline()
				table_name = table_name.strip()
				dictionary[table_name] = {}
				dictionary[table_name]['info'] = []
				attr = f.readline()
				attr = attr.strip()
				while attr != "<end_table>":
					dictionary[table_name]['info'].append(attr)
					attr = f.readline()
					attr = attr.strip()
			line = f.readline()
			line = line.strip()

	for table_name in dictionary:
		dictionary[table_name]['table'] = []
		dictionary[table_name]['name'] = table_name
		with open ('./' + table_name + '.csv', 'r') as f:
			for line in f:
				dictionary[table_name]['table'].append([int(field.strip('"')) for field in line.strip().split(',')])

def main():
	read_meta_data()
	query = sys.argv[1]
	query = query.strip('"').strip()
	query = query.replace("SELECT ", "select ")
	query = query.replace("DISTINCT ", "distinct ")
	query = query.replace("FROM ", "from ")
	query = query.replace("WHERE ", "where ")
	query = query.replace("AND ", "and ").replace("OR ", "or ").replace("MIN", "min").replace("MAX", "max").replace("AVG", "avg").replace("SUM", "sum")
	parse(query)

if __name__ == '__main__':
    main()