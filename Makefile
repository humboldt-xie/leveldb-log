all:main.cpp
	        g++ -g -I./ -Ileveldb-1.18/  -Ileveldb-1.18/include/ main.cpp leveldb-1.18/libleveldb.a  -lpthread
