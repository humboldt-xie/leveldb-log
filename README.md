
## 读日志

使用　SequentialFile 这个接口，读取文件

使用 Evn里面的NewSequentialFile 打开日志文件

```cpp
SequentialFile* file;
Status ok=env->NewSequentialFile(fname, &file);

```

使用log::Reader 读取日志文件:

```cpp
log::Reader *reader=new log::Reader(file, 
                                    NULL, 
                                    true/*checksum*/,
                                    initial_offset/*initial_offset*/);
while (reader->ReadRecord(&record, &scratch)){

}
initial_offset=reader->LastRecordOffset();
```

这样，日记记录已经读取出来了。

然后，需要转换成WriteBatch 才可以获取数据

```cpp
WriteBatch batch;
WriteBatchInternal::SetContents(&batch, record);
//获取当前记录的Sequence
const SequenceNumber cur_seq =WriteBatchInternal::Sequence(&batch);
```

获取数据库日志文件列表:

```cpp
std::vector<std::string> filenames;
env->GetChildren("./testdb", &filenames);
std::sort(filenames.begin(), filenames.end());
uint64_t number;
FileType type;
string fname;
for (size_t i = 0; i < filenames.size(); i++) {
 if (ParseFileName(filenames[i], &number, &type)) {
	 if(type==kLogFile){
		 fname="testdb/"+filenames[i];
		 PrintLog(env,fname);
	 }
 }
}

```

## 问题

最后一条记录，当读到最后一条记录之后，文件到末尾

结束，等待。然后重新打开文件，会重复读到最后一条记录


