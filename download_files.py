import urllib

def download_file(url,file_name):
        testfile = urllib.URLopener()
        testfile.retrieve(url, file_name)
        return





str1 = "http://downloads.dbpedia.org/2015-10/core-i18n/"
str2 = "/"
str3 = ".ttl.bz2"

name = raw_input()

lang = "en"
download_file( str1+lang+str2+name+"_"+lang+str3,"/downloads/"+file_name+"_"+lang)

name = raw_input()

while name != "end":
        file_name = name
        lang = "en"
        download_file( str1+lang+str2+name+"_"+lang+str3,"/downloads/"+file_name+"_"+lang)
        lang = "de"
        download_file( str1+lang+str2+name+"_"+lang+str3,"/downloads/"+file_name+"_"+lang)
        lang = "es"
        download_file( str1+lang+str2+name+"_"+lang+str3,"/downloads/"+file_name+"_"+lang)
        lang = "fr"
        download_file( str1+lang+str2+name+"_"+lang+str3,"/downloads/"+file_name+"_"+lang)
        lang = "ja"
        download_file( str1+lang+str2+name+"_"+lang+str3,"/downloads/"+file_name+"_"+lang)
        lang = "nl"
        download_file( str1+lang+str2+name+"_"+lang+str3,"/downloads/"+file_name+"_"+lang)
        name = raw_input()








