
#include "email.h"
#include "pgquery.h"
#include "pgdb.h"
#include "preq.h"
#include "mailq.h"

namespace dm {
    
int ums_exec_oid = INT_NULL;
list<preq*> preq::preqs;
    
pg::binbuf&
preq::configure(const string& q, int _params_nr)
{
	pt_bb.reset();
	
	if ( INT_NULL != plan_id ) {
		errlog << "set_options on plan which is alrady planned: " << info_str() << "\n";
		exit(-1);
	}

	params_nr = _params_nr;
	sql = q;

	return pt_bb;
}
    
    
bool
preq::release_plan(void)
{
	if ( INT_NULL == plan_id )
		return true;

	pg::query q;
	if ( !q.exec("select ums_release_plan(" + itos(plan_id) + ")")) {
		errlog << "could not release plan for query " << info_str() << "\n";
	}

	plan_id = INT_NULL;
	return true;
}
    
    
bool
preq::compile(pg::query& q)
{
	// release the plan if it's already defined, keep plan_id
	if ( INT_NULL != plan_id && !q.exec ("select ums_release_plan(" + itos(plan_id) + ")")) {
		errlog << "could not release plan for query " << info_str();
		return false;
	}
	
	q.prepare("select ums_prepare($1,$2,$3,$4)", 4);
	q.bind() << plan_id << sql << params_nr << pt_bb;

	int old_plan_id = plan_id;
	plan_id = INT_NULL;

	if (!q.exec() || !q.next()) {
		errlog << "ERROR: ums_prepare failed for query=" << info_str() << "\n";
		return false;
	}

	plan_id = q.getInt(0);
	li << "plan_id=" << plan_id << " for query=" << info_str() << "\n";
	
	if (old_plan_id != INT_NULL && plan_id != old_plan_id)
		errlog << "ERROR: got plan_id=" << plan_id << " old=" << old_plan_id << "\n";

	return true;
}
    
    
string
preq::info_str(void) const
{
	return "[sql=\"" + sql + "\" params_nr=" + itos(params_nr) + " plan_id=" +
	    (INT_NULL == plan_id ? string("<unplanned>") : itos(plan_id)) + "]";
}
    
    
// global plans
preq restore_mailq_plan;
preq tempblock_ip_plan;
preq update_message_stat_plan;
preq update_client_stat_plan;
preq update_sender_conf_plan;
preq update_month_plan;
preq update1_ml_plan;
preq log_bounce_plan;
preq add2stoplist_plan;
preq log_to_sender_stat_plan;
preq delete_from_mailq_mt_plan;
preq mailq_mt_reinject_plan;
preq inc_ms_queued_nr_plan;
preq mt_email_log_plan;
preq register_delivery_plan;
    
    
bool
preq::init(void)
{
	pg::query q;
	q.prepare("select oid from pg_proc where proname='ums_exec'");
	if (!(q.exec() && q.next())) {
		errlog << "Failed to lookup Oid for the \"ums_exec\" function!\n";
		return false;
	}
	ums_exec_oid = q.getInt(0);
	li << "ums_exec_oid=" << ums_exec_oid << "\n";
	
	
	restore_mailq_plan.configure("select restore_mailq_r($1,$2,$3,$4,$5,$6,$7)", 7)
	    << INT4OID << INT4OID << INT4OID << INT4OID << INT4OID << INT4OID << INT4OID;
	if (!restore_mailq_plan.compile(q))
		return false;
	
	tempblock_ip_plan.configure("select block_domain(id,$2) from domain where id=$1 and tempblocked_at is NULL", 2)
	    << INT4OID << TEXTOID;
	if (!tempblock_ip_plan.compile(q))
		return false;
	
	update_message_stat_plan.configure
	    ( "update message_stat set sent_nr=sent_nr+$1,"
	      "undelivered_nr=undelivered_nr+$2,"
	      "bounced_nr=bounced_nr+$3,"
	      "soft_bounced_nr=soft_bounced_nr+$4,"
	      "delivery_attempts_nr=delivery_attempts_nr+$5,"
	      "mx_attempts_nr=mx_attempts_nr+$6,"
	      "tm=extract('epoch' from now())::integer"
	      " where message_id=$7", 7)
	    << INT4OID << INT4OID << INT4OID << INT4OID << INT4OID << INT4OID << INT4OID;
	if (!update_message_stat_plan.compile(q))
		return false;
	
	update_month_plan.configure
	    ("select update_month_quota($1)", 1) << TEXTOID;
	if (!update_month_plan.compile(q))
		return false;

	update_sender_conf_plan.configure
	    ("update sender_conf set value = $1 where name = 'ums_curr_used'", 1) << INT4OID;
	if (!update_sender_conf_plan.compile(q)) {
		return false;
	}

	update_client_stat_plan.configure
	    ("update client_stat set emails_sent_nr=emails_sent_nr+$1,"
	       "bw_used=bw_used+$2, "
	       "bw_left=(CASE WHEN bw_left is NULL THEN NULL ELSE bw_left-$2 END), "
	       "emails_left=(CASE WHEN emails_left is NULL THEN NULL ELSE emails_left-$1 END) "
	       "where client_id=$3", 3)
	    << INT4OID << INT4OID << INT4OID;
	if (!update_client_stat_plan.compile(q))
		return false;
	
	update1_ml_plan.configure("update mailinglist set ss_nr=ss_nr-$1,ss_normal_nr=ss_normal_nr-$1 where id=$2", 2)
	    << INT4OID << INT4OID;
	if (!update1_ml_plan.compile(q))
		return false;
	
	log_bounce_plan.configure("insert into bounces (email,message_id,type) values ($1,$2,10)", 2)
	    << TEXTOID << INT4OID;
	if (!log_bounce_plan.compile(q))
		return false;
	
	add2stoplist_plan.configure("insert into stoplist (email,mm_id,ml_id,client_id,log) values ($1,$2,$3,$4,$5)", 5)
	    << TEXTOID << INT4OID << INT4OID << INT4OID << TEXTOID;
	if (!add2stoplist_plan.compile(q))
		return false;
	
	log_to_sender_stat_plan.configure("insert into sender_stat (tm,client_id,message_id,delivered_nr,bounced_nr,bw,domain_id,dg_id) values ($1,$2,$3,$4,$5,$6,$7,$8)", 8)
	    << INT4OID << INT4OID << INT4OID << INT4OID << INT4OID << INT4OID << INT4OID << INT4OID;
	if (!log_to_sender_stat_plan.compile(q))
		return false;
	
	delete_from_mailq_mt_plan.configure("delete from mailq_mt where id=$1", 1)
	    << INT4OID;
	if (!delete_from_mailq_mt_plan.compile(q))
		return false;
	
	mailq_mt_reinject_plan.configure ("update mailq_mt set is_loaded=false where id=$1", 1)
	    << INT4OID;
	if (!mailq_mt_reinject_plan.compile(q))
		return false;
	
	inc_ms_queued_nr_plan.configure("update message_stat set queued_nr=queued_nr+$1 where message_id=$2", 2)
	    << INT4OID << INT4OID;
	if (!inc_ms_queued_nr_plan.compile(q))
		return false;
	
        mt_email_log_plan.configure("insert into mt_rcpt_log (mt_rcpt_id,seed,mail_content_type,md_ip,ct_ip,log) values ($1,$2,$3,$4,$5,$6)", 6)
            << INT4OID << INT4OID << INT4OID << TEXTOID << TEXTOID << TEXTOID;
        if (!mt_email_log_plan.compile(q))
		return false;
	
	register_delivery_plan.configure("select * from register_delivery($1, $2)", 2)
	    << INT4OID << INT4OID;
	if (!register_delivery_plan.compile(q))
		return false;

	return true;
}
    
    
bool
preq::replan_all(void)
{
	
	li << "!! replanning all queries\n";
	
	// locking mailq since finalizer thread, under mailq lock, checks preq-s for validity
	qmgr.lock();
	
	pg::query q;
	
	for (list<preq*>::iterator it = preqs.begin(); it != preqs.end(); it++) {
		if ( !!**it                // it is compiled
			&& !(*it)->compile(q) // and it can't be recompiled
		) {
			errlog << "ERROR: failed to re-plan preq " << (*it)->info_str() << "\n";
			DIE();
		}
	}
	
	li << "==> replanned " << preqs.size() << " queries\n";
	
	qmgr.unlock();
	return true;
}
    
    
// calls "replan_all" no faster than every 10 seconds
// call this function from the loader
bool
preq::replan_all_if(void)
{
	static int last_tm = 0;

	if (!last_tm) {
		last_tm = dm::curr_time();
		return true;
	}

	if (dm::curr_time() - last_tm < 10)
		return true;

	last_tm = dm::curr_time();
	return replan_all();
}
    
    
bool
preq::serialize(pg::binbuf& bb)
{
	bb << plan_id << sql << params_nr << pt_bb;
	return !!bb;
}


bool
preq::load(pg::binbuf& bb, pg::query& q)
{
	if (INT_NULL != plan_id) {
		errlog << "trying to load preq from a binbuf to a preq object which is already planned or has plan_id set (to " << plan_id << ")\n";
		return false; //NOP
	}
	
	bb >> plan_id >> sql >> params_nr >> pt_bb;
	if (!bb) {
		errlog << "ERROR: failed to read preq from binbuf\n";
		return false;
	}
	
	q.prepare("select ums_prepare($1,$2,$3,$4)", 4);
	q.bind() << plan_id << sql << params_nr << pt_bb;
	
	if (!q.exec() || !q.next()) {
		errlog << "NOTE: ums_prepare failed for query=" << info_str() << "; could not load this plan, planning as dummy\n";
		return plan_as_dummy(q);
	}

	int new_plan_id = q.getInt(0);
	if (new_plan_id != plan_id) {
		errlog << "ERROR: re-planned under different plan_id => DIE\n";
		return false;
	}

	return true;
}
    
    
bool
preq::plan_as_dummy(pg::query& q)
{
	// create dummy sql
	if ( params_nr > 0 ) {
		sql = "select $1";
		for (int i = 1; i < params_nr; i++) {
			sql += ",$";
			sql += itos(i+1);
		}
	    
	} else
		sql = "select 1";

	q.prepare("select ums_prepare($1,$2,$3,$4)", 4);
	q.bind() << plan_id << sql << params_nr << pt_bb;

	if (!q.exec() || !q.next()) {
		errlog << "ERROR: failed to plan dummy sql=\"" << sql << "\"\n";
		DIE(); // this is an internal error
		return false;
	}

	int new_plan_id = q.getInt(0);
	if (new_plan_id != plan_id) {
		errlog << "ERROR: re-planned under different plan_id => DIE\n";
		DIE();
		return false;
	}

	return true;
}
};
