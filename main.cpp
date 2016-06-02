/*
 * =====================================================================================
 *
 *       Filename:  main.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  06/01/16 09:46:20
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (), 
 *        Company:  
 *
 * =====================================================================================
 */
#include <assert.h>
#define LEVELDB_PLATFORM_POSIX 
#include "db/dbformat.h"
#include "leveldb/db.h"
#include "leveldb/write_batch.h"
#include "leveldb/env.h"
#include "db/log_reader.h"
#include "db/write_batch_internal.h"
#include "db/filename.h"
#include <string>
#include <iostream>
#include <ctime>
#include <algorithm>
using namespace std;
using namespace leveldb;

class PrintTable: public WriteBatch::Handler {
 public:
  virtual void Put(const Slice& key, const Slice& value) {
    //value.data()
    string skey(key.data(),key.size());
    printf("put %s \n",skey.c_str());
  }
  virtual void Delete(const Slice& key) {
    printf("delete %s\n",key.data());
  }
};

void PrintLog(Env * env,string fname){
 int64_t initial_offset=0;
 while(1){
   SequentialFile* file;
   Status ok=env->NewSequentialFile(fname, &file);
   printf("%s\n",ok.ToString().c_str());
   if(!ok.Ok()){
     return;
   }
   log::Reader *reader=new log::Reader(file, 
                                       NULL, 
                                       true/*checksum*/,
                                       initial_offset/*initial_offset*/);
   std::string scratch;
   WriteBatch batch;
   Slice  record;
   while (reader->ReadRecord(&record, &scratch) ) {
     if (record.size() < 12) {
       continue;
     }
     PrintTable printTable;
     WriteBatchInternal::SetContents(&batch, record);
     const SequenceNumber cur_seq =WriteBatchInternal::Sequence(&batch);
     const SequenceNumber last_seq =
         WriteBatchInternal::Sequence(&batch) +
         WriteBatchInternal::Count(&batch) - 1;
     batch.Iterate(&printTable);
     printf("%llu\n",cur_seq );
   }
   printf("last record %lld\n",reader->LastRecordOffset());
   initial_offset=reader->LastRecordOffset();
   sleep(1);
 }

}


void *thread(void *ptr) {
 leveldb::DB* db;
 leveldb::Options options;
 options.block_size = 1400;
 options.create_if_missing = true;
 leveldb::Status status = leveldb::DB::Open(options, "./testdb", &db);
 
 string key="key";
 string value="value";
 int index=0;
 while(1) {
   //printf("writing:\n");
   for(int i=0; i<10; i++){
     char buf[255];
     sprintf(buf,"%d",index);
     key=buf;
     for(int j=0; j<1000; j++){
       sprintf(buf,"%d-%d",index,j);
       value+=buf;
     }
     status = db->Put(leveldb::WriteOptions(), key,value);
     if(!status.ok()){
     }
     index++;
   }
   printf("end write:%d\n",index);
   sleep(5);
 }

}


int main()
{
  pthread_t id;
  int i,ret;
  ret=pthread_create(&id,NULL,thread,NULL);

 leveldb::Options options;
 options.block_size = 1400;
 options.create_if_missing = true;


 while(1){
   Env* env = options.env;
   std::vector<std::string> filenames;
   env->GetChildren("./testdb", &filenames);
   std::sort(filenames.begin(), filenames.end());
   uint64_t number;
   FileType type;
   string fname="testdb/000021.log";
   for (size_t i = 0; i < filenames.size(); i++) {
     if (ParseFileName(filenames[i], &number, &type)) {
       if(type==kLogFile){
         fname="testdb/"+filenames[i];
         PrintLog(env,fname);
       }
     }
   }
   sleep(1);
 }

 sleep(1000);

  return 0;
}
