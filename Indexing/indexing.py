import lucene

lucene.initVM()
 
INDEXIDR = self.__index_dir
 
indexdir = SimpleFSDirectory(File(INDEXIDR))
 
analyzer = CJKAnalyzer(Version.LUCENE_30)
 
index_writer = IndexWriter(indexdir, analyzer, True, IndexWriter.MaxFieldLength(512))
 
document = Document()
 
document.add(Field("content", str(page_info["content"]), Field.Store.NOT, Field.Index.ANALYZED))
 
document.add(Field("url", visiting, Field.Store.YES, Field.Index.NOT_ANALYZED))
 
document.add(Field("title", str(page_info["title"]), Field.Store.YES, Field.Index.ANALYZED))
 
index_writer.addDocument(document)
 
index_writer.optimize()
 
index_writer.close()