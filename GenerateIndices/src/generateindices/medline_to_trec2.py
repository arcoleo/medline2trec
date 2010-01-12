#!/usr/bin/env python


'''

Directions -

Generally you first generate a regular indri index.  To do this in parallel
(8-core) do:

$ ./medline_to_trec2 --master


Target TREC Format:

<?xml version="1.0"?>

<ROOT>
    <DOC>
        <DOCNO>pmid:16403481</DOCNO>
        <PMID>16403481</PMID>
        <ISSN>1350-4177</ISSN>
        <PUBDATE>2007 Jan</PUBDATE>
        <SOURCE>Untrasonics sonochemistry</SOURCE>
        <TITLE>Are sonochemically prepared alpha-amylase protein microspheres biologically active?</TITLE>
        <ABSTRACT>Using the high-intensity untrassound, ...</ABSTRACT>
        <AUTHOR>Avivi-Levi-S : Gedanken-A :</AUTHOR>
        <CHEMICALS>Enzymes, Immobilized: alpha-Amylase : </CHEMICALS>
        <MESH>Enzymes Activation : Enzymes, Immobilized : Microspheres : Sonication : alpha-Amylase :</MESH>
    </DOC>
    <DOC>
        ...
    </DOC>
</ROOT>

'''


import os
import sys
# FIXME: only works if run as ./medline_to_trec2.py
sys.path.append('../spaces/')
sys.path.append('.')
from lxml import etree
import multiprocessing
import sqlite3
import MySQLdb
import MySQLdb.cursors
import subprocess
import pprint
import gzip
import time
import zipfile
import glob
import codecs
import unicodedata
from optparse import OptionParser
from StringIO import StringIO
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, \
    PassiveDefault, Text, UnicodeText, Unicode, exceptions
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, create_session
from sqlalchemy.orm.exc import NoResultFound
#from medline_db import Base, trecdoc

global db_cursor, conn, unicode_type, global_set
global_set = set()

BASE_DIR = '../../../..'

unicode_type = type(u'')
encode_codec = 'ascii'
encode_normalize_array = ['NFC', 'NFKC', 'NFD', 'NFKD']
encode_normalize = encode_normalize_array[3]

Base = declarative_base()
class trecdoc(Base):
    __tablename__ = 'tredocs'

    id = Column(Integer, primary_key=True)
    pmid = Column(String(25), unique=True)
    #fullname = Column(String)
    #password = Column(String)

    def __init__(self, pmid):
        self.pmid = pmid
        #self.fullname = fullname
        #self.password = password

    def __repr__(self):
        return "<User('%s')>" % (self.pmid)


def unicode_to_ascii(input_string):
    if type(input_string) == type(str()):
        #print 'Doing nothing', input_string
        return input_string
    try:
        #print 'encoding', input_string
        #unicode_string = unicodedata.normalize(encode_normalize, input_string)
        unicode_string = unicodedata.normalize(encode_normalize, input_string).encode('ascii', 'ignore')
        #print 'Encoded', unicode_string
    except Exception, ex:
        print 'unicode_to_ascii Error', ex
        print type(input_string)
        print input_string
        sys.exit(1)
    return unicode_string

def pop_unique_docno(docno):
    try:
        result = session.query(trecdoc).filter(trecdoc.pmid==docno).one()
        #if docno in global_set:
        #    return True
        #return False
        
        #print ('result',result)
        #result2 = session.delete(result)
        #print ('result2', result2)
    except NoResultFound, ex:
        return False
    except NameError:
        # not using sessions
        pass
    except Exception, ex:
        exc_class, exc, traceback = sys.exc_info()
        print 'Exception pop_unique_docno: ', exc_class
        print exc
        print traceback
        sys.exit()


    return True
    
    #sys.exit()
    
def divide_list(min, max, num_sets):
    #x = range(1,592+1)
    #y = 10
    x = range(min, max+1)
    y = num_sets
    s = len(x)/y    # size of normal sublists
    e = len(x) - y*s    # extra elements for last sublist
    z = [x[s*i:s*i+s] for i in range(y-1)] + [x[-s-e:]]
    #print z
    zz = ()
    for i in z:
        zz.append((i[0], i[-1]))
    return zz


def dictlist(max_size=10**5):
    global curr_size
    curr_size = 0
    res = {}
    res['MedlineCitationSet'] = []
    
    counter = 0
    old_elem = None
    for event, elem in etree.iterparse(parse_file):
        if elem.tag in ['MedlineCitation']:
            res['MedlineCitationSet'] = []
            counter += 1 
            xmltodict(elem, res['MedlineCitationSet'], max_size)
            elem.clear()
    print '\n\n\ncitation_dict\n'
    print len(citation_dict)
    reply = {}
    return reply

def xmltodict(node, res, max_size=10):
    global curr_size, citation_dict

    keep_attrib = False
    rep = {}
    if len(node):
        for n in list(node):
            rep[node.tag] = []
            if curr_size < max_size:
                value = xmltodict(n, rep[node.tag], max_size)
            if len(n) and curr_size < max_size:
                #value = {'value':rep[node.tag],'attributes':n.attrib, 'tail':n.tail}
                if keep_attrib and n.attrib and len(n.attrib):
                    value = {'value': rep[node.tag],'attributes':n.attrib}
                else:
                    value = {'value': rep[node.tag]}
                if value:
                    res.append({n.tag:value})
            else :
                if len(rep[node.tag]):
                    res.append(rep[node.tag][0])
        if node.tag == 'MedlineCitation':
            tmp_pmid = u'pmid-' + str(res[0]['PMID']['value'])
            if tmp_pmid in citation_dict:
                return
            b = MedlineNode()
            b.extract_citation(res)
            
            tmp_citation = b.get_dict()
            try:
                tmp_trec_record = trecdoc(tmp_citation['DOCNO'])
                try:
                    #global_set.add(tmp_citation['DOCNO'])
                    session.add(tmp_trec_record)
                    session.commit()
                except NameError:
                    pass
                except exceptions.IntegrityError, ex:
                    session.rollback()
                    if not 'Duplicate entry' in str(ex):
                        print 'IntegrityError', exc.orig
                        sys.exit()
                    else:
                        pass
                except Exception, ex:
                    exc_class, exc, traceback = sys.exc_info()
                    print 'Session.add', exc_class
                    print exc
                    sys.exit(2)
            except Exception, ex:
                exc_class, exc, traceback = sys.exc_info()
                
                print 'Session Exception'
                print exc_class
                print exc
                #print traceback
                sys.exit(2)
            else:
                curr_size += 1
                citation_dict[b.get_docno()] = b.get_dict()
            #session.commit()
            
            value = 0
    else:
        #return
        # value 'item' only added here, no recursion
        value = {}
        #value = {'value':node.text,'attributes':node.attrib,'tail':node.tail}
        try:
            value_int = int(node.text)
        except Exception, ex:
            value_int = node.text
        if keep_attrib and node.attrib and len(node.attrib):
            value = {'value': value_int, 'attributes': node.attrib}
        else:
            value = {'value': value_int}
        res.append({node.tag:value})
        if node.tag == 'MedlineCitation':
            curr_size += 1
    return


class MedlineNode:
    """
    Foo
    """
    
    def __init__(self):
        self.name = u'David'
        self.data_dict = {}
        self.docno = u''
        self.pmid = 0
        self.source = u''
        self.title = u''
        self.abstract = u''
        self.issn = u''
        self.chemicals = []
        self.mesh = []
        self.pubdate = u''
        self.authors = []
        self.genes = []
        self.keywords = []
    
    def get_name(self):
        return self.name
    
    def get_docno(self):
        return self.docno
    
    def extract_pmid(self, item):
        self.pmid = item['PMID']['value']
        self.docno = 'pmid-' + str(item['PMID']['value'])
        return self.docno
    
    def extract_source(self, item):
        try:
            if item['Article']['value'][0]['Journal']['value'] == None:
                return self.source
        except Exception, ex:
            return self.source
        for attr in item['Article']['value'][0]['Journal']['value']:
            if 'Title' in attr and (not attr['Title']['value'] == None):
                try:
                    self.source = attr['Title']['value']
                except Exception, ex:
                    self.source =  attr['Title']['value']
        return self.source
    
    def extract_title(self, item):
        try:
            if item['Article']['value'] == None:
                return self.title
        except Exception, ex:
            return self.title
        for attr in item['Article']['value']:
            if 'ArticleTitle' in attr and \
                   (not attr['ArticleTitle']['value'] == None):
                self.title = unicode_to_ascii(attr['ArticleTitle']['value'])
                #try:
                    #self.title = unicodedata.normalize(encode_normalize, attr['ArticleTitle']['value']).encode(encode_codec, 'ignore')
                #except TypeError, ex:
                    #self.title = attr['ArticleTitle']['value']
        return self.title
    
    def extract_abstract(self, item):
        if 'Article' in item.keys():
            for attr in item['Article']['value']:
                if 'Abstract' in attr:
                    for sub_attr in attr['Abstract']['value']:
                        if 'AbstractText' in sub_attr:
                            # FIXME: utf8
                            self.abstract = unicode_to_ascii(sub_attr['AbstractText']['value'])
                            #try:
                            #    self.abstract = sub_attr['AbstractText']['value']
                            #except Exception, ex:
                            #    print ''
                            #    print ('type', type(sub_attr['AbstractText']['value']))
                            #    print ('unicode', ex)
                            #self.abstract = unicode_to_ascii(sub_attr['AbstractText']['value'])
                            #try:
                                #self.abstract = unicodedata.normalize(encode_normalize, sub_attr['AbstractText']['value']).encode(encode_codec, 'ignore')
                            #except Exception, ex:
                                #self.abstract = sub_attr['AbstractText']['value']
        if not len(self.abstract):
            for attr in item:
                if 'OtherAbstract' in attr:
                    try:
                        if item['OtherAbstract']['value'] == None:
                            return self.abstract
                    except Exception, ex:
                        print 'Exception', ex
                        return self.abstract
                    for sub_attr in item['OtherAbstract']['value']:
                        if 'AbstractText' in sub_attr:
                            self.abstract = sub_attr['AbstractText']['value']
        return self.abstract
    
    def extract_issn(self, item):
        try:
            if item['Article']['value'][0]['Journal']['value'] == None:
                return self.issn
        except Exception, ex:
                return self.issn
        for attr in item['Article']['value'][0]['Journal']['value']:
            if 'ISSN' in attr and (not attr['ISSN']['value'] == None):
                self.issn = attr['ISSN']['value']
        return self.issn
    
    def extract_chemicals(self, item):
        try:
            if item['ChemicalList']['value'] == None:
                return self.chemicals
        except Exception, ex:
            return self.chemicals
        for attr in item['ChemicalList']['value']:
            if 'Chemical' in attr:
                for sub_attr in attr['Chemical']['value']:
                    if 'NameOfSubstance' in sub_attr:
                        self.chemicals.append(sub_attr['NameOfSubstance']['value'])
        return self.chemicals
    
    def extract_mesh(self, item):
        if item['MeshHeadingList']['value'] == None:
            return self.mesh
        for attr in item['MeshHeadingList']['value']:
            if 'MeshHeading' in attr:
                for sub_attr in attr['MeshHeading']['value']:
                    if 'DescriptorName' in sub_attr:
                        self.mesh.append(sub_attr['DescriptorName']['value'])
        return self.mesh

    def extract_genes(self, item):
        if item['GeneSymbolList']['value'] == None:
            return self.genes
        for attr in item['GeneSymbolList']['value']:
            if 'GeneSymbol' in attr:
                self.genes.append(attr['GeneSymbol']['value'])
        return self.genes

    def extract_keywords(self, item):
        if item['KeywordList']['value'] == None:
            return self.keywords
        for attr in item['KeywordList']['value']:
            if 'Keyword' in attr:
                self.keywords.append(attr['Keyword']['value'])
        return self.keywords
    
    def extract_pubdate(self, item):
        try:
            if item['Article']['value'][0]['Journal']['value'] == None:
                return self.pubdate
        except Exception, ex:
            return self.pubdate
        for attr in item['Article']['value'][0]['Journal']['value']:
            if 'JournalIssue' in attr:
                for sub_attr in attr['JournalIssue']['value']:
                    if 'PubDate' in sub_attr:
                        for date_attr in sub_attr['PubDate']['value']:
                            if 'Year' in date_attr:
                                self.pubdate_year = str(date_attr['Year']['value'])
                            if 'Month' in date_attr:
                                self.pubdate_month = str(date_attr['Month']['value'])        
        try:
            self.pubdate = self.pubdate_year
        except Exception, ex:
            pass
        try:
            self.pubdate = self.pubdate + u' ' + self.pubdate_month
        except Exception, ex:
            pass
        return self.pubdate
    
    def extract_author(self, item):
        if item['Article']['value'] == None:
            return self.authors
        for attr in item['Article']['value']:
            if 'AuthorList' in attr:
                for author in attr['AuthorList']['value']:
                    self.curr_last_name = u''
                    self.curr_initials = u''
                    # FIXME: for some reason, AuthorList can be at times {'value': '\n'} instead of None
                    try:
                        for name in author['Author']['value']:
                            if 'LastName' in name:
                                self.curr_last_name = name['LastName']['value']
                            if 'Initials' in name:
                                self.curr_initials = name['Initials']['value']
                    except Exception, ex:
                        pass
                    self.authors.append((self.curr_last_name, self.curr_initials))
        return self.authors
    
    def extract_citation(self, item):
        for value in item:
            if 'PMID' in value:
                self.extract_pmid(value)
            elif 'Article' in value:
                self.extract_source(value)
                self.extract_author(value)
                self.extract_issn(value)
                self.extract_pubdate(value)
                self.extract_title(value)
                self.extract_abstract(value)                
            elif 'OtherAbstract' in value:
                self.extract_abstract(value)
            elif 'ChemicalList' in value:
                self.extract_chemicals(value)
            elif 'MeshHeadingList' in value:
                self.extract_mesh(value)
            elif 'GeneSymbolList' in value:
                self.extract_genes(value)
            elif 'KeywordList' in value:
                self.extract_keywords(value)

    def get_dict(self):
        return {'PMID': self.pmid, 'DOCNO':self.docno, 'ISSN':self.issn,
                'PUBDATE':self.pubdate, 'SOURCE':self.source,
                'TITLE':self.title, 'ABSTRACT':self.abstract,
                'AUTHOR':self.authors, 'CHEMICALS':self.chemicals,
                'MESH':self.mesh}


def init_db_structure():
    global session
    print ('init_db_structure', 'begin')
    buf = StringIO()
    mock = None
    #mock = 'mock'
    engine_str = 'mysql://bswriter:bswriter@localhost:3306/beespace_abstract?charset=utf8&use_unicode=0'

    try:
        if mock:
            engine = create_engine(engine_str, strategy=mock, 
                executor=lambda s, p=u'': buf.write(s + p))
        else:
            engine = create_engine(engine_str, echo=False)
    except Exception, ex:
        print 'Create engine', ex
        sys.exit()
    metadata = Base.metadata
    
    try:
        metadata.create_all(engine)
    except Exception, ex:
        exc_class, exc, traceback = sys.exc_info()
        print 'Exception: ', exc_class
        print exc
        print traceback
        sys.exit(1)
    if mock:
        print buf.getvalue()
    try:
        Session = sessionmaker(bind=engine)
        session = Session()
    except Exception, ex:
        exc_class, exc, traceback = sys.exc_info()
        print 'Exception', exc_class
        print exc
        print traceback
        sys.exit(1)

    print ('init_db_structure', 'end')
    session.commit()
    #u1 = trecdoc('pmid-123456789')
    #session.add(u1)
    #session.commit()
    #sys.exit()


def init_db(db = 'mysql'):
    '''Initialize database connection
    '''
    
    global cursor, conn
    
    hostname = 'localhost'
    database = 'beespace_abstract'
    #databaseName = curr_sql
    username = 'bswriter'
    password = 'bswriter'

    print ('db', db)
    if db == 'mysql':
        try:
            conn = MySQLdb.connect(host=hostname, db=database,
                user=username, passwd=password,
                cursorclass=MySQLdb.cursors.DictCursor)
                #use_unicode=True, charset='utf8')
        except NameError, ex:
            print "Could not connect to database: %s" % str(ex)
            sys.exit(1)
        cursor = conn.cursor()
        print 'Connected to mysql'

    if db == 'sqlite':
        try:
            conn = sqlite3.connect('./sql2/medline_temp.sqlite')
            conn.row_factory = sqlite3.Row
            conn.text_factory = str
        except NameError, ex:
            try:
                print "Could not connect to database: %s" % str(ex)
            except NameError, ex:
                print "Could not connect to database: %s" % ex
            sys.exit(1)
        except Exception, ex:
            #print "Unexpected error:", sys.exc_info()[0]
            print "Unexpected error: %s : %s" %  (str(type(ex)), ex)
            sys.exit(1)
        else:
            cursor = conn.cursor()
            print 'Connected to db'

    try:
        init_db_structure()
        #cursor.execute('''create table trec (docno text, pmid text, issn text, pubdate text, source text, title text, abstract text, author text, chemicals text, mesh text)''')
    except Exception, ex:
        print ex
    return cursor, conn


def make_indri_param_file(corpus_path, param_file_path):
    try:
        param_str = "<parameters>\n\
        <memory>8G</memory>\n\
        <index>non_repo_data/Medline/current/indri</index>\n\
        <corpus>\n\
            <class>trectext</class>\n\
            <path>%s</path>\n\
        </corpus>\n\
        <field>\n\
            <name>docno</name>\n\
        </field>\n\
        <field>\n\
            <name>pmid</name>\n\
        </field>\n\
        <field>\n\
            <name>issn</name>\n\
        </field>\n\
        <field>\n\
            <name>pubdate</name>\n\
        </field>\n\
        <field>\n\
            <name>source</name>\n\
        </field>\n\
        <field>\n\
            <name>title</name>\n\
        </field>\n\
        <field>\n\
            <name>abstract</name>\n\
        </field>\n\
        <field>\n\
            <name>author</name>\n\
        </field>\n\
        <field>\n\
            <name>chemicals</name>\n\
        </field>\n\
        <field>\n\
            <name>genes</name>\n\
        </field>\n\
        <field>\n\
            <name>mesh</name>\n\
        </field>\n\
        <field>\n\
            <name>keywords</name>\n\
        </field>\n\
</parameters>\n" % corpus_path
    except Exception, ex:
        print 'Exception ', ex
        print corpus_path
        return
    try:
        fp = open(param_file_path, 'w')
    except Exception, ex:
        print 'Exception ', ex
        return
    fp.write(param_str)
    fp.close()


def record_to_trec(record, annotator=False):
    """Write TREC field"""
    #print '\n', ('record_to_trec', 'begin')

    if annotator:
        trec_str = u"\
<DOC>\n\
<DOCNO>%(DOCNO)s</DOCNO>\n\
<TEXT>%(TITLE)s\n\
%(ABSTRACT)s</TEXT>\n\
</DOC>\n\
" % record
    else:
        #print ('record_to_trec', 'non_annotator')
        doc = etree.Element('DOC')
        etree.SubElement(doc, 'DOCNO').text = record['DOCNO']
        etree.SubElement(doc, 'PMID').text = str(record['PMID'])
        etree.SubElement(doc, 'ISSN').text = record['ISSN']
        etree.SubElement(doc, 'PUBDATE').text = record['PUBDATE']
        etree.SubElement(doc, 'SOURCE').text = record['SOURCE']
        etree.SubElement(doc, 'TITLE').text = record['TITLE']
        etree.SubElement(doc, 'ABSTRACT').text = record['ABSTRACT']
        etree.SubElement(doc, 'AUTHOR').text = record['AUTHOR']
        etree.SubElement(doc, 'CHEMICALS').text = record['CHEMICALS']
        etree.SubElement(doc, 'GENES').text = record['GENES']
        etree.SubElement(doc, 'MESH').text = record['MESH']
        etree.SubElement(doc, 'KEYWORDS').text = record['KEYWORDS']

        #print ('record_to_trec', 'end subelement')
        #trec_str_pretty = etree.tostring(doc, pretty_print=True)
        trec_str = '<DOC>\n'
        for element in doc.iter():
            if (not element == doc):
                trec_str += etree.tostring(element) + '\n'
        trec_str += '</DOC>\n\n'
        #trec_str = unicode(trec_str)
        #print ('record_to_trec', 'end trec_str')
        
        
        trec_str_old = u"\
<DOC>\n\
<DOCNO>%(DOCNO)s</DOCNO>\n\
<PMID>%(PMID)s</PMID>\n\
<ISSN>%(ISSN)s</ISSN>\n\
<PUBDATE>%(PUBDATE)s</PUBDATE>\n\
<SOURCE>%(SOURCE)s</SOURCE>\n\
<TITLE>%(TITLE)s</TITLE>\n\
<ABSTRACT>%(ABSTRACT)s</ABSTRACT>\n\
<AUTHOR>%(AUTHOR)s</AUTHOR>\n\
<CHEMICALS>%(CHEMICALS)s</CHEMICALS>\n\
<GENES>%(GENES)s</GENES>\n\
<MESH>%(MESH)s</MESH>\n\
<KEYWORDS>%(KEYWORDS)s</KEYWORDS>\n\
</DOC>\n\n\
" % record

    #print trec_str
    #print ('record_to_trec', 'end')

    return trec_str


# 1. Medline -> dictionary

def author_to_str(pm_author):
    author_str = u''
    for author_tuple in pm_author:
        for author_name in author_tuple:
            author_str = author_str + author_name + '-'
        # remove last '-'
        author_str = author_str[:-1]
        author_str = author_str + ' : '
    # remove last ' : '
    author_str = author_str[:-3]
    return author_str

def chemicals_to_str(pm_chemicals):
    #print ('chemicals_to_str', pm_chemicals)
    chemicals_str = u''
    for chemical in pm_chemicals:
        chemicals_str = chemicals_str + chemical + ' : '
    chemicals_str = chemicals_str[:-3]
    return chemicals_str


def dictionary_to_db():
    '''Dict -> db to eliminate dupes.  Takes no additional time to write db.'''
    print ('dictionary_to_db', 'begin')

    count = 0
    update_queue = []
    for key, value in citation_dict.iteritems():
    #for key, value in citation_dict.items()[:10]:
        count += 1
        #print ('abstract', key, value)
        #if count > 5:
        #    sys.exit()
        citation = models_sa.Citation(value['DOCNO'], pmid=str(value['PMID']).decode(),
                pubdate=value['PUBDATE'], source=value['SOURCE'],
                title=value['TITLE'], abstract=value['ABSTRACT'],
                author=author_to_str(value['AUTHOR']),
                chemicals=chemicals_to_str(value['CHEMICALS']),
                mesh=chemicals_to_str(value['MESH']))
        try:
            session.add(citation)
        except Exception:
            exc_class, exc, traceback = sys.exc_info()
            print ('exc_class', exc_class)
            print ('exc', exc)
            print ('value', value)
            sys.exit(1)
        # NOTE: the faster you commit, the less you hold in ram
        # NOTE: on 1st zip, commiting every 10k means prog rakes 42 and 173M ram
        # NOTE: on 30k, takes same time and 333M
        # NOTE: on 5k, takes 42s and 152M
        #if not (count % 1):
        if True:
            #print 'Commit'
            try:
                session.commit()
            except Exception, InvalidRequestError:
                print 'InvalidRequestError ----'
                exc_class, exc, traceback = sys.exc_info()
                print ('exc_class', exc_class)
                print ('exc', exc)
                #print ('value', value)
                session.rollback()
                try:
                    citation_mod = session.query(models_sa.Citation).filter_by(
                        pmid=str(value['PMID']).decode()).one()
                    citation_mod.pubdate=value['PUBDATE']
                    citation_mod.source=value['SOURCE']
                    citation_mod.title=value['TITLE']
                    citation_mod.abstract=value['ABSTRACT']
                    citation_mod.author=author_to_str(value['AUTHOR'])
                    citation_mod.chemicals=chemicals_to_str(value['CHEMICALS'])
                    citation_mod.mesh=chemicals_to_str(value['MESH'])
                    session.add(citation_mod)
                    session.commit()
                except Exception:
                    print 'Update error'
                    exc_class, exc, traceback = sys.exc_info()
                    print ('exc_class', exc_class)
                    print ('exc', exc)
                    sys.exit(1)
                finally:
                    print 'Second try worked'
            except Exception, IntegrityError:
                print 'IntegrityError ----'
                exc_class, exc, traceback = sys.exc_info()
                print ('exc_class', exc_class)
                print ('exc', exc)
                sys.exit(1)
            except Exception, ex:
                errno = 0
                exc_class, exc, traceback = sys.exc_info()
                print ('exc_class', exc_class)
                print ('exc', exc)
                sys.exit(1)
    try:
        session.commit()
    except Exception, IntegrityError:
        exc_class, exc, traceback = sys.exc_info()
        try:
            # FIXME: handle rollbacks, etc
            if 'Duplicate entry' in exc[0]:
                pass
            else:
                print ('exc_class', exc_class)
                print ('exc', exc)
                sys.exit(1)
        except Exception, ex:
            print ('exc_class', exc_class)
            print ('exc', exc)
            sys.exit(1)
    except Exception, ex:
        print 'Unknown Error'
        exc_class, exc, traceback = sys.exc_info()
        print ('exc_class', exc_class)
        print ('exc', exc)
        sys.exit(1)
    #print count, ' failures'
    print ('dictionary_to_db', 'end')

# 3. db -> TREC
def db_to_trec(curr_trec, annotator=False):
    global cursor, conn
    
    print ('db_to_trec', 'begin', curr_trec)
    trec_dict = {}
    # NOTE: This section writes the trec file directly from the dict and skips the db.  This could cause problems with dupes
    header = '<?xml version="1.0"?>\n\n<ROOT>\n'

    if not options.small_trec:
        curr_trec_final = curr_trec + '.trec'
        try:
            fp = codecs.open(curr_trec_final, 'w', encode_codec)
        except Exception, ex:
            print 'db_to_trec Exception', ex
            print ('curr_trec', curr_trec)
            sys.exit()
        fp.write(header)
    small_trec_size = 1000
    small_trec_file_iter = 1
    small_trec_iter = 0
    for key, value in citation_dict.iteritems():
        if options.small_trec and (not small_trec_iter):
            nm = '%02d' % (small_trec_file_iter)
            small_trec_file_iter_str = curr_trec + '-' + \
                nm + '.trec'
            try:
                fp = codecs.open(small_trec_file_iter_str, 'w', encode_codec)
                
                #try:
                    #unicode_str = unicodedata.normalize(encode_normalize, header).encode(encode_codec, 'ignore')
                #except Exception, ex:
                    #print 'Unicode', ex
                    #sys.exit()
                #else:
                fp.write(unicode_to_ascii(header))
            except Exception, ex:
                print 'db_to_trec Exception', ex
                print ('curr_trec', curr_trec)
                sys.exit()
        if annotator:
            citation = {
                'DOCNO': value['DOCNO'],
                'TITLE': value['TITLE'],
                'ABSTRACT': value['ABSTRACT']}
            s_success = True
        else:
            try:
                citation = {
                    'DOCNO': value['DOCNO'],
                    'PMID': value['PMID'],
                    'ISSN': value['ISSN'],
                    'PUBDATE': value['PUBDATE'],
                    'SOURCE': value['SOURCE'],
                    'TITLE': value['TITLE'],
                    'ABSTRACT': value['ABSTRACT'],
                    'AUTHOR': author_to_str(value['AUTHOR']),
                    'CHEMICALS': chemicals_to_str(value['CHEMICALS']),
                    'GENES': chemicals_to_str(value.get('GENE', '')),
                    'MESH': chemicals_to_str(value['MESH']),
                    'KEYWORDS': chemicals_to_str(value.get('KEYWORDS', ''))}
                s_success = True
            except Exception, ex:
                print ('value', value)
                s_success = False
        if s_success:
            if pop_unique_docno(value['DOCNO']):
                trec_str = record_to_trec(citation, annotator)
                fp.write(unicode_to_ascii(trec_str))
            #try:
                
                #fp.write(unicodedata.normalize(encode_normalize, trec_str))
                #fp.write(unicodedata.normalize(encode_normalize, trec_str.encode(encode_codec, 'ignore')))
            #except UnicodeEncodeError, ex:
                #print '\n'
                #print ('UnicodeEncodeError', ex), '\n'
                #print ('citation', citation)
                #sys.exit()
            #except Exception, ex:
               #print 'UnicodeData1', ex
               #print type(trec_str.encode(encode_codec, 'ignore'))
               #print trec_str
               #sys.exit()
        small_trec_iter += 1
        if options.small_trec and (small_trec_iter == small_trec_size):
            fp.write('</ROOT>\n')
            fp.close()
            small_trec_iter = 0
            small_trec_file_iter += 1
    if not options.small_trec:
        fp.write('</ROOT>\n')
        fp.close()
    return


def xml_to_dictionary():
    '''Converts the xml structure created by lxml to a proper python dict'''
    global records, citation_dict, parse_file
    #print ('xml_to_dictionary', 'begin')
    res = dictlist()
    #print ('xml_to_dictionary', 'end')


def load_compressed_file():
    global res, records, citation_dict, trec_file, parse_file 

    print ('load_compressed_file', xml_file)
    if any(xml_file.endswith(x) for x in ('.gzip', '.gz')):
#        r_xml_file = gzip.open(xml_file, 'rb')
        parse_file = gzip.open(xml_file, 'rb')
        #parse_file = r_xml_file
    elif xml_file.endswith('.zip'):
        r_xml_file = zipfile.ZipFile(xml_file, 'r')
        for info in r_xml_file.infolist():
            print info.filename
            parse_file = info.filename

    print 'parsing xml'
    #tree = etree.parse(parse_file)
    print 'converting to dict'
    # FIXME: this part takes forever.  Optimize.
    #res = xml_to_dict3.dictlist(tree.getroot())
    print 'done'
    #del tree

    records = 0
    citation_dict = {}
    #b = MedlineNode()
    #xml_to_dictionary()


def medline_to_dictionary():
    print ('medline_to_dictionary', 'begin')
    load_compressed_file()
    xml_to_dictionary()
    print 'Len:', len(citation_dict)


def trec_to_indri(indri_param_file):
    cmd = ['/var/repo/hg-igb/inst/mod-indri-2.4/bin/buildindex', 
        indri_param_file]
    print 'cmd', cmd
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    print ('stdout', stdout)
    #print ('stderr', stderr)


def run_entity(xml_source_file, entity_out_file, sentence_out_file,
               field_list_file):
    exec_path = os.path.join(BASE_DIR, 'source', 'BeeSpace3', 'Entity', '1.1',
        'scripts', 'tagEntities.sh')
    cmd = [exec_path, xml_source_file, entity_out_file, sentence_out_file,
           field_list_file]
    print 'cmd', cmd
    if not options.dry_run:
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        print ('stdout', stdout)
        print ('stderr', stderr)
    else:
        print 'Dry run'


class do_pmid_set:
    def __init__(self):
        if not options.pmid_set:
            return
        self.loop_input_files()

    def open_xml(self):
        pass

    def loop_input_files(self):
        for xml_index, xml_file in enumerate( \
            xml_gz_glob[int(options.start)-1:int(options.end)]):
            print xml_index, xml_file


    
def update_status(file_list, file_name):
    fp = open(file_name, 'a')
    for i in file_list:
        fp.write(i + '\n')
    fp.close()


def slave(id):
    print ('id', id)
    task_list = []
    for task in core_list[id]:
        print (id, task)
        try:
            start = '--start=' + str(task)
            end = '--end=' + str(task)
            cmd = ['./medline_to_trec2.py', start, end]
            if options.base_dir:
                cmd.append('--base_dir='+options.base_dir)
            if options.annotator:
                cmd.append('--annotator')
            if options.entity:
                cmd.append('--entity')
            if options.dry_run:
                cmd.append('--dry_run')
            if options.small_trec:
                cmd.append('--small_trec')
            if options.pmid_set:
                cmd.append('--pmid_set')
            print (id, cmd)
            subprocess.check_call(cmd)
            #p = multiprocessing.Process(target=subprocess.check_call, args=(cmd,))
            #p.start()
            task_list.append(task)
        except Exception, ex:
            print ('err', id, ex)
    return (id, task_list)
    
    

def spawn_slaves():
    num_running = 0
    print 'spawn_slaves'
    cores = int(options.cores)
    if options.odd or options.even:
        cores *= 2
    pool = multiprocessing.Pool(processes=cores)
    #result = pool.apply_async(slave, [8])
    #print result.get(timeout=5)
    #
    completed_tasks = pool.map(slave, range(cores))
    print(completed_tasks)
    
    #pool.apply()
    print 'continueing...'
    sys.exit()

def init_options():
    global options, parse_range, xml_gz_glob, LIMIT, core_list, cores, BASE_DIR
    usage = "Usage: ./medline_to_trec2.py --start=num1 --end=num2 \
(where num1 < num2)"
    parser = OptionParser(usage)
    parser.add_option('-v', '--verbose', action='store_true', dest='verbose',
        help='verbose output')
    parser.add_option('-b', '--base_dir', action='store', dest='base_dir',
        help='dir which is parent of source, non_repo_master, etc')
    parser.add_option('-s', '--start', action='store', dest='start')
    parser.add_option('-e', '--end', action='store', dest='end')
    parser.add_option('-m', '--master', action='store_true', dest='master')
    parser.add_option('-i', '--indri', action='store_true', dest='indri',
        help='build indri')
    parser.add_option('-a', '--annotator', action='store_true', dest='annotator',
        help='build annotator')
    parser.add_option('-t', '--entity', action='store_true', dest='entity',
        help='buid entities')
    parser.add_option('-d', '--dry_run', action='store_true', dest='dry_run',
        help='dry run')
    parser.add_option('--odd', action='store_true', dest='odd',
        help='process only odds')
    parser.add_option('--even', action='store_true', dest='even',
        help='process only evens')
    parser.add_option('--small_trec', action='store_true', dest='small_trec',
        help='write out trec files with no more than 1k elements')
    # FIXME: if cores option used, must add subprocesses, etc
    parser.add_option('-c', '--cores', action='store', dest='cores', default=8)
    parser.add_option('--init_db', action='store_true', dest='init_db', default=False)
    parser.add_option('--pmid_set', action='store_true', dest='pmid_set', default=False)
    (options, args) = parser.parse_args()
    print ('options', options)
    print ('args', args)
    if options.init_db:
        init_db_structure()
        sys.exit()
    # FIXME: should reset db on --master or something
    if options.pmid_set:
        init_db_structure()
    LIMIT = 0
    #JOBS = 593
    JOBS = 887
    if options.base_dir:
        BASE_DIR = os.path.expanduser(options.base_dir)
    xml_gz_path = os.path.join(BASE_DIR, 'non_repo_data', 'current', 'Medline',
        'gz')
    xml_gz_glob_path = os.path.join(xml_gz_path, '*.xml.gz')
    #print ('xml_gz_glob_path', xml_gz_glob_path)
    xml_gz_glob = sorted(glob.glob(xml_gz_glob_path))
    #print ('xml_gz_glob', len(xml_gz_glob))
    parse_range = []

    odd_even_offset = 1
    # auto-split over cores
    if options.cores:
        cores = int(options.cores)
        if options.odd or options.even:
            cores *= 2
        print ('cores', cores)
        if options.master:
            print 'master'
            core_list = []
            for core in range(1, cores+1):
                core_list.append((range(core, JOBS+1, cores)))
            print(core_list[0])


    if options.start and options.end:
        for x in range(int(options.start), int(options.end)+1):
            num = '%04d' % x
            parse_range.append(str(num))
    
   # print ('parse_range', parse_range)
    #LIMIT = int(options.end) - int(options.start) + 1
    #print ('LIMIT', LIMIT, int(options.start), int(options.end))
    #sys.exit(1)
    #print 'glob', xml_gz_glob


def load_position():
    global COMPLETED_LIST
    COMPLETED_LIST = []
    return
    try:
        fp = open('medline.status')
    except Exception, ex:
        print 'Cannot load medline.status'
        return
    for completed_file in fp.readlines():
        COMPLETED_LIST.append(completed_file.strip())
    #print 'completed_list', COMPLETED_LIST

init_options()

curr_trec = {}
curr_trec_annotator = {}

if options.master:
    spawn_slaves()
    sys.exit()

#t1 = do_pmid_set()
#sys.exit()

if options.annotator:
    print 'TREC -> Annotator Indri'
    for xml_index, xml_file in enumerate( \
        xml_gz_glob[int(options.start)-1:int(options.end)]):
        print (xml_index, xml_file)
        curr_id = xml_file[37:-7]
        str_postfix = xml_file.rfind('.xml.gz')
        curr_id = xml_file[str_postfix-4:str_postfix]
        print ('annotator', ('xml_index', xml_index), ('xml_file', xml_file),
            ('curr_id', curr_id), xml_file[38:-7], ('rfind', str_postfix))
        # FIXME: abs path
        curr_trec_annotator[xml_index] = (xml_file,
            os.path.join(BASE_DIR, 'non_repo_data', 'current', 'Medline', \
                'trec_annot', 'annot-' + curr_id))
        if xml_index in curr_trec_annotator:
            print ('xml_index', xml_index)
            if not options.dry_run:
                medline_to_dictionary()
                #param_file = '/opt2/raid/data/Medline/2009/trec_annot/indri-annot-param-' + xml_file[32:-7] + '.xml'
                #print ('param_file', param_file)
                #print ('make_indri_param_file', curr_trec[xml_index][1], param_file)
                #make_indri_annot_param_file(curr_trec[xml_index][1], param_file)
                #trec_to_indri(param_file)
                db_to_trec(curr_trec_annotator[xml_index][1], annotator=True)
    sys.exit(0)

if options.entity:
    field_list_file = os.path.join(BASE_DIR, 'non_repo_data', 'current',
        'Medline', '2009', 'entity', 'TIAB.txt')
    # NOTE: must be run after trec annotator files have been generated
    print 'Generate Entities'
    for xml_index, xml_file in enumerate( \
        xml_gz_glob[int(options.start)-1:int(options.end)]):
        curr_id = int(xml_file[37:-7])
        print ('entity', ('xml_index', xml_index), ('xml_file', xml_file),
            ('curr_id', curr_id), xml_file[37:-7])
        id_str = '%04d' % curr_id
        # FIXME: abs path
        xml_source_file = os.path.join(BASE_DIR, 'non_repo_data',
            'current', 'Medline', 'trec_entity',
            'test-' + id_str + '.trec')
        entity_out_file = os.path.join(BASE_DIR, 'non_repo_data', 'current',
            'Medline', 'entity', id_str + '.entity')
        sentence_out_file = os.path.join(BASE_DIR, 'non_repo_data', 'current',
            'Medline', 'entity', id_str + '.sentence')
        if os.path.exists(xml_source_file):
            run_entity(xml_source_file, entity_out_file, sentence_out_file,
                       field_list_file)
    sys.exit()

if options.indri:
    # FIXME: I think this is depreciated
    print 'TREC -> Indri'
    for xml_index, xml_file in enumerate( \
        xml_gz_glob[int(options.start)-1:int(options.end)]):
        #print (xml_index, xml_file)
        curr_id = xml_file[37:-7]
        print ('indri', ('xml_index', xml_index), ('xml_file', xml_file),
            ('curr_id', curr_id), xml_file[38:-7])
        curr_trec[xml_index] = (xml_file, os.path.join(BASE_DIR,
            'non_repo_data', 'current', 'Medline', 'trec',
            'test-' + curr_id + '.trec'))
        if xml_index in curr_trec:
            param_file = os.path.join(BASE_DIR, 'non_repo_data', 'current',
                'Medline', 
                'indri-param-' + xml_file[34:-7] + '.xml')
            print ('param_file', param_file)
            print ('make_indri_param_file', curr_trec[xml_index][1], param_file)
            if not options.dry_run:
                make_indri_param_file(curr_trec[xml_index][1], param_file)
                trec_to_indri(param_file)
    sys.exit(0)


#cursor, conn = init_db()
load_position()
LIMIT = 10 ** 3

if not (options.master or options.annotator):
    print 'First occurance of not master or annotator'
    for xml_index, xml_file in enumerate( \
        xml_gz_glob[int(options.start)-1:int(options.end)]):
        #print '--dsa'
        #print (xml_index, xml_file)
        curr_id = xml_file[37:-7]
        # FIXME: *major hack*
        curr_id = curr_id[-4:]
        # print ('not master or annot', ('xml_index', xml_index),
            #('xml_file', xml_file), ('curr_id', curr_id), xml_file[38:-7])
        if not options.dry_run:
            print '\n\n[%d] %s' % (xml_index, curr_id)
            if (curr_id in COMPLETED_LIST) or (curr_id not in parse_range):
                if curr_id in COMPLETED_LIST:
                    print 'Skipping', curr_id, 'because completed'
                else:
                    print 'skipping', curr_id
                #print ('parse_range', parse_range)
                continue
            else:
                print 'Working on', curr_id
                #print COMPLETED_LIST[:10]
                #sys.exit()
            print '\n\n[%d] %s' % (xml_index, curr_id) 
            #curr_sql = './sql2/medline-' + curr_id + '.sqlite'
            #curr_trec[xml_index] = './trec2/test-' + curr_id + '.trec'
            # FIXME: abs path
            curr_trec[xml_index] = (xml_file, os.path.join( \
                BASE_DIR, 'non_repo_data', 'current', 'Medline', 'trec_lxml', \
                'test-' + curr_id))
            medline_to_dictionary()
    
            #dictionary_to_db()
            #COMPLETED_LIST.append(curr_id)
            #update_status(COMPLETED_LIST, 'medline.status')
            print 'db_to_trec'
            #db_to_trec(curr_trec[xml_index][1])


if not (options.master or options.annotator):
    print 'db_to_trec'
    if not options.dry_run:
        print ('curr_trec', curr_trec)
        for xml_index, xml_file in enumerate( \
            xml_gz_glob[int(options.start)-1:int(options.end)]):
            print xml_index, xml_file
            if xml_index in curr_trec:
                print '!!  ', xml_index, curr_trec[xml_index]
                db_to_trec(curr_trec[xml_index][1])
    
