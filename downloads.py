import urllib2;
import StringIO;
from lxml import etree;
from lxml.html.soupparser import fromstring;
from multiprocessing import Pool, freeze_support
import itertools;

def u(str):
	return str.encode('utf-8').strip();

def downloadurl(url):
	try:
		response = urllib2.urlopen(url);
		tree = fromstring(response.read());
	except Exception as e:
		print "%s"%e
		print url
		tree = fromstring("");
	return tree;

# Map from state names to its two letter codes
def twoletterstates():
	twolettercode = {}
	with open("states.txt") as f:
		str = f.readlines();
		for aline in str:
			twolettercode[aline.split("\t")[0].strip()] = aline.split("\t")[1].strip()
		return twolettercode

def moststates():
	cities = []
	states = []
	twolettercode = twoletterstates();
	with open("most.csv") as f:
		str = f.readlines();
		for aline in str:
			city = aline.strip().split("\t")[0].split("[")[0];
			state = aline.strip().split("\t")[1];
			if state in twolettercode:
				cities.append(city)
				states.append(twolettercode[state])
	return (cities,states)

def crawlmanualpage(query,state,city,pageno):
	url = "http://www.yelp.com/search?find_desc={0}&find_loc={1}+{2}&start={3}".format("+".join(query.split()),"+".join(city.split()),state,(pageno-1)*10);
	tree = downloadurl(url);

	pagedump = [];

	blocks = tree.xpath("//div[@class='biz-listing-large']");
	for block in blocks:
		dump = {}
		dump["brand"] = "";
		dump["streetaddress"]="";
		dump["addresslocality"]="";
		dump["state"]="";
		dump["zipcode"]="";
		dump["telephone"]="";
		dump["rating"]=dump["pricerange"]="";

		for a in block.xpath("div[@class='main-attributes']"):
			for element in a.xpath("div//h3[@class='search-result-title']//a"):
				dump["brand"] = element.text_content();
			for element in a.xpath("div/div[@class='media-story']/div[@class='price-category']/span/span/text()"):
				dump["pricerange"] = element;
			for element in a.xpath("div/div/div/div/i/@title"):
				dump["rating"] = element;
		for a in block.xpath("div[@class='secondary-attributes']"):
			for sa in a.xpath("address//text()[1]"):
				dump["streetaddress"] = sa.strip();
			for ad in a.xpath("address//text()[2]"):
				dump["addresslocality"] = ad.split(",")[0];
				dump["state"] = ad.split(",")[1].split()[0];
				dump["zipcode"] = ad.split(",")[1].split()[1];
			dump["telephone"] = a.xpath("span[@class='biz-phone']")[0].text_content()
		pagedump.append(dump)
	writefile = open("e/{0}-{1}-{2}.txt".format(state,city,pageno),"wt");
	templ = "";
	for adump in pagedump:
		temp = "{0}|{1}|{2}|{3}|{4}|{5}|{6}|{7}\n".format(u(adump["brand"]),u(adump["streetaddress"]),u(adump["addresslocality"]),u(adump["state"]),u(adump["zipcode"]),u(adump["telephone"]),u(adump["rating"]),u(adump["pricerange"]));
		templ = templ + temp;
	writefile.write(templ);
	writefile.close();


def cmpstar(query_state_city_pageno):
	crawlmanualpage(*query_state_city_pageno);

if __name__ == '__main__':
	freeze_support();	#http://stackoverflow.com/questions/5442910/python-multiprocessing-pool-map-for-multiple-arguments
	pc = 10;
	pagec = 6;
	pool = Pool(pc);

	querylist = []
	pagenolist = []
	citylist = []
	statelist = []

	(cities,states) = moststates();
	index = 0
	for city in cities:
		citylist.extend([city]*pagec)
		statelist.extend([states[index]]*pagec)
		querylist.extend(["lawn mower repair"]*pagec)
		pagenolist.extend(range(1,pagec+1))
		index = index + 1
	pool.map(cmpstar, itertools.izip(querylist,statelist,citylist,pagenolist));
