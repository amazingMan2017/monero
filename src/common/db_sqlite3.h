//
// Created by mac on 2019/4/27.
//

#ifndef MONEROCLASSIC_DB_SQLITE3_H
#define MONEROCLASSIC_DB_SQLITE3_H

#pragma once

#include <atomic>
#include <sqlite3.h>
#include <sqlite3.h>
#include <string>
#include <serialization/keyvalue_serialization.h>


struct st_blockcreate_statistics
{
	uint64_t  blockheight;
	uint64_t  difficulty;
	uint64_t 	create_template_time;
	uint64_t	notify_block_time;
	std::string block_hash;
	std::string block_nonce;
};

struct st_nextdifficulty_statistics
{
	uint64_t blockheight;
	uint64_t timespan;
	uint64_t totalwork;
	uint64_t difficulty;
	std::string logtime;

	BEGIN_KV_SERIALIZE_MAP()
		KV_SERIALIZE(blockheight)
		KV_SERIALIZE(timespan)
		KV_SERIALIZE(totalwork)
		KV_SERIALIZE(difficulty)
		KV_SERIALIZE(logtime)
	END_KV_SERIALIZE_MAP()
};

/**
 * just use for statistic
 * */
class BlockchainSQLITEDB{


public:
    BlockchainSQLITEDB();
    ~BlockchainSQLITEDB();
    /**
     * open db & create | delete tables
     * */
    int open(const std::string& filename, const int mdb_flags=0);

    /**
     * close db
     * */
    int close();

    /**
    * Turn on the statistics switch
    * */
    void open_statistics();

    /**
    * Turn off the statistics switch
    * */
    void close_statistics();

    /**
     * is statistics opened
     * */
    bool is_statistics_open(){return m_statistics_open;};

    /**
     * statistics the parameter and result of next block difficulty
     * */
    int insert_next_difficulty(uint64_t blockheight,uint64_t timespan,uint64_t totalwork,uint64_t difficulty);

    /**
     * query difficulty statistics
     * */
    int query_next_difficulty(uint64_t from_height,uint64_t to_height,std::vector<st_nextdifficulty_statistics> results);

    /**
     * statistics the timespan from greating_block_template to notify_new_block
     * */
		int insert_block_statistics(uint64_t blockheight, uint64_t difficulty,uint64_t create_template_time);

		/**
		* update block_hash block_nonce notify_block_time with blockheight
		* */
		int update_block_statistics(uint64_t blockheight,std::string block_hash,std::string block_nonce, uint64_t notify_block_time);

		/**
		* query block create time
		* */
		int query_block_statistics(uint64_t from_height,uint64_t to_blockheight,std::vector<st_blockcreate_statistics> results);

private:
    bool m_statistics_open;
    sqlite3* m_sqlite3_db;
    sqlite3_stmt *m_sqlite3_stmt;

};



#endif //MONEROCLASSIC_DB_SQLITE3_H
