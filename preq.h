#ifndef __DMAILER_PREPAREDQUERIES_H_
#define __DMAILER_PREPAREDQUERIES_H_

#include "pgquery.h"
#include "pgbinder.h"
#include "logger.h"

namespace dm {
    
    // an enum with named slots for ums_prepare-d queries
    extern int ums_exec_oid;
    
    
    class preq {
	friend inline pg::binbuf& operator<< ( pg::binbuf& bb, preq& plan );
	
	pg::binbuf pt_bb; // binbuf with param types
	int plan_id;      // prepared query id
	string sql;       // sql query
	int params_nr;    // number of params
	
	static list<preq*> preqs; // global private list of all preq-s defined at this time
	
    public:
	preq ( void ) : plan_id(INT_NULL),params_nr(INT_NULL) {
	    // remember myself
	    preqs.push_back ( this );
	}
	
	~preq ( void ) {
	    release_plan();
	    preqs.remove ( this ); //NOTE: might be slow with many preq-s; optimize when this list grows over 1000 (?)
	}
	
	pg::binbuf& configure ( const string& q, int _params_nr );
	bool release_plan ( void );
	bool compile ( pg::query& );
	string info_str ( void ) const;
	
	static bool init ( void );
	
	// returns false if this plan has already been compiled and holds plan_id reference (which might be invalid in db)
	bool operator! ( void ) const { return INT_NULL == plan_id; }
	
	// re-plans all preq-s
	// rationale: adjust to newer analyze data
	static bool replan_all ( void );
	static bool replan_all_if ( void ); // not more frequent than so many seconds
	
	// serialize the preq into the binbuf
	bool serialize ( pg::binbuf& );
	
	// load preq from a binary buffer
	bool load ( pg::binbuf&, pg::query& );
	
    protected:
	bool plan_as_dummy ( pg::query& ); // create dummy plan for current plan_id, so that ums_exec will eat all params_nr parameters from pt_bb
    };
    
    
    inline pg::binbuf& operator<< ( pg::binbuf& bb, preq& plan ) {
	if ( INT_NULL == plan.plan_id ) {
	    errlog << "attempt to bind plan which is not planned; info=" << plan.info_str() << "\n";
	    return bb;
	}
	
	bb << (uint16_t)plan.plan_id;
	return bb;
    }
    
    
    extern preq restore_mailq_plan; // used for restoring stuff from BIDS_BIN
    extern preq tempblock_ip_plan; // update domain set tempblocked_at=... where id=$id
    extern preq update_message_stat_plan;
    extern preq update_client_stat_plan;
    extern preq update_sender_conf_plan;
    extern preq update_month_plan;
    extern preq update1_ml_plan;
    extern preq log_bounce_plan;           // insert into bounces
    extern preq add2stoplist_plan;         // insert into stoplist
    extern preq log_to_sender_stat_plan;   // insert new row into sender_stat
    extern preq delete_from_mailq_mt_plan; // delete from mailq_mt where id=$1
    extern preq mailq_mt_reinject_plan;    // updates mailq_mt.is_loaded with false
    extern preq inc_ms_queued_nr_plan;     // updates message_stat.queued_nr
    extern preq mt_email_log_plan;         // insert into message test log
    extern preq register_delivery_plan;    // commit delivery, move/add subscribers if needed
};

#endif // __DMAILER_PREPAREDQUERIES_H_
