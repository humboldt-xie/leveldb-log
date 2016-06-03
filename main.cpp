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
#include "env_custom.h"
#include "leveldb/env.h"
#include "db/log_reader.h"
#include "db/write_batch_internal.h"
#include "db/filename.h"
#include <string>
#include <iostream>
#include <ctime>
#include <algorithm>
#include "errno.h"
#include "fcntl.h"
using namespace std;
using namespace leveldb;

static Status IOError(const std::string& context, int err_number) {
  return Status::IOError(context, strerror(err_number));
}
class FollowSequentialFile: public SequentialFile {
 private:
  std::string filename_;
  int fd_;
  int64_t filesize_;

 public:
  FollowSequentialFile(const std::string& fname)
      : filename_(fname),fd_(-1) { }
  virtual ~FollowSequentialFile() { if(fd_>0)close(fd_); }

  Status Open(){
    int f = open(filename_.c_str(),O_RDONLY);
    if (f < 0) {
      return IOError(filename_, errno);
    } else {
      if(fd_>0){
        close(f);
      }
      fd_=f;
      filesize_=Size();
      return Status::OK();
    }
  }
  int64_t Size(){
      int64_t cur=lseek(fd_,0,SEEK_CUR);
      int64_t filesize_=lseek(fd_,0,SEEK_END);
      lseek(fd_,cur,SEEK_SET);
      return filesize_;
  }
  void Reset(){
    if(fd_>0){
      lseek(fd_,0,SEEK_SET);
    }
  }
  bool HasChange(){
    if(filesize_!=Size()){
      filesize_=Size();
      return true;
    }
    return false;
  }


  virtual Status Read(size_t n, Slice* result, char* scratch) {
    Status s;
    ssize_t r = read(fd_,scratch, n );
    if(r<0){
      // A partial read with an error: return a non-ok status
      s = IOError(filename_, errno);
    }else{
      if(r<n){
        // We leave status as ok if we hit the end of the file
      }
      *result = Slice(scratch, r);
    }
    return s;
  }

  virtual Status Skip(uint64_t n) {
    if (lseek(fd_, n, SEEK_CUR)<0) {
      return IOError(filename_, errno);
    }
    return Status::OK();
  }
};



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
uint64_t g_last_seq=0;


class LogIterator {
 public:
  std::vector<std::string> filenames;
  std::string curlog_;
  int64_t     number_;
  Env *env;
  SequenceNumber cur_seq_;
  SequenceNumber last_seq_;
  SequenceNumber lost_;
  uint64_t        offset;
  bool            isEof;
  std::string scratch;
  WriteBatch batch;
  Slice  record;
  FollowSequentialFile *file_;
  log::Reader *reader_;
  LogIterator (){
    file_=NULL;
    reader_=NULL;
    last_seq_=0;
    cur_seq_=0;
    lost_=0;
    offset=0;
    curlog_="";
  }
  void UpdateFilelist(){
    std::vector<std::string> filenames;
    env->GetChildren("./testdb", &filenames);
    std::sort(filenames.begin(), filenames.end());
    FileType type;
    uint64_t number;
    this->filenames.clear();
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type)) {
        if(type==kLogFile){
          this->filenames.push_back(filenames[i]);
        }
      }
    }
  }
  bool NextLog() {
    FileType type;
    uint64_t number;
    if(curlog_==""){
      if(filenames.size()>0){
        if(ParseFileName(filenames[0], &number, &type)){
          curlog_=filenames[0];
          this->number_=number;
          offset=0;
          return true;
        }
        return false;
      }else{
        return false;
      }
    }
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type)) {
        if(number>this->number_){
          this->number_=number;
          curlog_=filenames[i];
          offset=0;
          return true;
        }
      }
    }
    return false;

  }
  Status Open() {
      if(reader_){
        delete reader_;
        reader_=NULL;
      }
      if(file_){
        delete file_;
        file_=NULL;
      }
      file_=new FollowSequentialFile("testdb/"+curlog_);
      Status ok=file_->Open();
      if(!ok.ok()){
        delete file_;
        file_=NULL;
        return ok;
      }
      offset=0;
      isEof=false;
      reader_=new log::Reader(file_, 
                            NULL, 
                            true/*checksum*/,
                            offset/*initial_offset*/);
      return Status::OK();
  }
  Status Reset() {
      if(reader_){
        delete reader_;
        reader_=NULL;
      }
      file_->Reset();
      reader_=new log::Reader(file_, 
                            NULL, 
                            true/*checksum*/,
                            offset/*initial_offset*/);
      return Status::OK();

  }
  bool PrepareReader() {
    UpdateFilelist();
    if(curlog_=="" || isEof){
      if(file_){
        if(file_->HasChange()){
          Reset();
          isEof=false;
          return false;
        }
      }
      if(NextLog()){
        Status s=this->Open();
        if(!s.ok()){
          return false;
        }
        return PrepareReader();
      }
      return false;
    }
    return true;
  }
  void CloseReader(){
    if(file_){
      delete file_;
    }
    if(reader_){
      delete reader_;
    }
    reader_=NULL;
    file_=NULL;
  }

  bool Next() {
    if(!PrepareReader()){
      return false;
    }
    while (reader_->ReadRecord(&record, &scratch) ) {
      if (record.size() < 12) {
        continue;
      }
      WriteBatchInternal::SetContents(&batch, record);
      SequenceNumber cur_seq=WriteBatchInternal::Sequence(&batch);
      SequenceNumber last_seq= 
          WriteBatchInternal::Sequence(&batch) +
          WriteBatchInternal::Count(&batch) - 1;
      if(cur_seq<=last_seq_){
        continue;
      }
      /*if(cur_seq!=last_seq_+1){
        printf("lost:%lld pre:%lld cur:%lld\n",lost_,cur_seq_,last_seq_);
      }*/
      lost_=cur_seq-(last_seq_+1);
      cur_seq_=cur_seq;
      last_seq_=last_seq;
      offset=reader_->LastRecordOffset();
      return true;
    }
    offset=reader_->LastRecordOffset();
    isEof=true;
    return false;
  }

};


void PrintLog(Env * env,string fname){
  int64_t initial_offset=0;
  FollowSequentialFile file(fname);
  while(1){
    //SequentialFile* file;
    //Status ok=env->NewSequentialFile(fname, &file);
    Status ok=file.Open();
    if(!ok.ok()){
      printf("%s\n",ok.ToString().c_str());
      return;
    }
    log::Reader *reader=new log::Reader(&file, 
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
      if(cur_seq<=g_last_seq){
        printf("skip seq:%lld last:%lld\n",cur_seq,g_last_seq);
        continue;
      }
      const SequenceNumber last_seq =
          WriteBatchInternal::Sequence(&batch) +
          WriteBatchInternal::Count(&batch) - 1;
      //batch.Iterate(&printTable);
      //printf("%llu\n",cur_seq );
      if(cur_seq!=g_last_seq+1){
        printf("pre:%lld cur:%lld\n",g_last_seq,cur_seq);
      }
      g_last_seq=last_seq;
    }
    printf("last record %s %lld %lld eof:%d\n",fname.c_str(),reader->LastRecordOffset(),g_last_seq);
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
      value="";
      for(int j=0; j<8000; j++){
        sprintf(buf,"%d-%d",index,j);
        value+=buf;
      }
      status = db->Put(leveldb::WriteOptions(), key,value);
      if(!status.ok()){
        printf("error:%s\n",status.ToString().c_str());
      }
      index++;
    }
    printf("end write:%d\n",index);
    sleep(2);
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

  LogIterator iter;
  iter.env=options.env;
  while(1){
    while(iter.Next()){
      printf("last record %s %lld %lld lost:%lld\n",iter.curlog_.c_str(),iter.offset,iter.last_seq_,iter.lost_);
      if(iter.lost_>0){
        printf("lost %lld ============\n",iter.lost_);
        sleep(1);
      }
    }
    printf("-last record %s %lld %lld eof:%d\n",iter.curlog_.c_str(),iter.offset,iter.last_seq_,iter.isEof);
    sleep(1);
  }


  /*while(1){
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
  }*/

  sleep(1000);

  return 0;
}
