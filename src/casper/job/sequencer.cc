/**
* @file sequencer.cc
*
* Copyright (c) 2011-2020 Cloudware S.A. All rights reserved.
*
* This file is part of casper-job-sequencer.
*
* casper-job-sequencer is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* casper-job-sequencer  is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU Affero General Public License
* along with casper-job-sequencer.  If not, see <http://www.gnu.org/licenses/>.
*/

#include "casper/job/sequencer.h"

#include "ev/postgresql/reply.h"
#include "ev/postgresql/value.h"
#include "ev/postgresql/error.h"

#include "cc/macros.h"
#include "cc/types.h"

CC_WARNING_TODO("CJS: review all comments and parameters names");
CC_WARNING_TODO("CJS: WRITE SOME OF THE DEBUG MESSAGES TO PERMANENT LOG!!");
CC_WARNING_TODO("CJS: check if v8 calls must be done on 'Main' thread");

CC_WARNING_TODO("CJS: REVIEW CC_DEBUG_LOG_TRACE / CC_DEBUG_LOG_MSG (\"job\") USAGE")

#ifdef __APPLE__
#pragma mark - casper::job::Sequencer
#endif

const char* const casper::job::Sequencer::s_schema_ = "js";
const char* const casper::job::Sequencer::s_table_  = "sequencer";
const std::map<std::string, casper::job::sequencer::Status> casper::job::Sequencer::s_irj_teminal_status_map_ = {
    { "completed", casper::job::sequencer::Status::Done      },
    { "failed"   , casper::job::sequencer::Status::Failed    },
    { "cancelled", casper::job::sequencer::Status::Cancelled }
};

/**
 * @brief Default constructor.
 *
 * param a_tube
 * param a_loggable_data
 * param a_config
 */
casper::job::Sequencer::Sequencer (const char* const a_tube, const ev::Loggable::Data& a_loggable_data, const cc::job::easy::Job::Config& a_config)
    : cc::job::easy::Job(a_loggable_data, a_tube, a_config),
      thread_id_(cc::debug::Threading::GetInstance().CurrentThreadID()),
      sequence_config_(a_config.other()[a_tube]), activity_config_(a_config.other()[a_tube])
{
    jfw_.omitEndingLineFeed();
    script_    = nullptr;
    log_level_ = static_cast<size_t>(std::max((int)a_config.log_level(), (int)log_level_));
}

/**
 * @brief Destructor
 */
casper::job::Sequencer::~Sequencer ()
{
    CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);
    // ... cancel any subscriptions ...
    ExecuteOnMainThread([this] {
         // ... unsubscribe from REDIS ...
        ::ev::redis::subscriptions::Manager::GetInstance().Unubscribe(this);
    }, /* a_blocking */ true);
    // ... forget v8 script ...
    if ( nullptr != script_ ) {
        delete script_;
    }
}

#ifdef __APPLE__
#pragma mark -
#endif

/**
 * @brief One-shot initialization.
 */
void casper::job::Sequencer::Setup ()
{
    CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);
    
    // ... prepare v8 simple expression evaluation script ...
    script_ = new casper::job::sequencer::v8::Script(/* a_owner */ tube_, /* a_name */  tube_ + "." + std::to_string(config_.instance()),
                                                     /* a_uri */ "thin air", /* a_out_path */ logs_directory()
    );
    // ... load it now ...
    script_->Load(/* a_external_scripts */ Json::Value::null, /* a_expressions */ {});
    //
    // SPECIAL CASE: we're interested in cancellation signals ( since we're running activites in sequence )
    //
    SetSignalsChannelListerer(std::bind(&casper::job::Sequencer::OnJobsSignalReceived, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
}

#ifdef __APPLE__
#pragma mark -
#endif

/**
 * @brief Register job sequence and it's activities.
 *
 * @param a_jid      Sequence info.
 * @param a_payload  Sequence payload.
 *
 * @return \link ActivityInfo \link of the first activity to be launched.
 */
casper::job::sequencer::Activity casper::job::Sequencer::RegisterSequence (sequencer::Sequence& a_sequence, const Json::Value& a_payload)
{
    CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);
    
    const sequencer::Tracking tracking = SEQUENCER_TRACK_CALL(a_sequence.bjid(), "REGISTERING SEQUENCE");
    
    // ... log ...
    SEQUENCER_LOG_SEQUENCE(CC_JOB_LOG_LEVEL_INF, a_sequence, CC_JOB_LOG_STEP_IN, "%s", "Registering");
    
    //
    // FORMAT:
    //
    // {
    //   "id" : <string>, - last component of to a_jrid
    //   "tube": <string>,
    //   "jobs" : [{
    //        "tube":     , <string>       - tube name,
    //        "ttr":      , <unsigned int> - time to run ( in seconds ),
    //        "validity": , <unsigned int> - validity ( in seconds ),
    //        "payload":  , <object>        - job payload
    //   }]
    // }
    //
    
    CC_WARNING_TODO("CJS: ensure id is the last component of a_jrid");
    
    // ... first, validate sequence payload ...
    Json::UInt seq_ttr      = 0;
    Json::UInt seq_validity = 0;
    
    try {
        
        const char* const seq_iomkmp = "Invalid or missing sequence ";

        Json::UInt seq_ttr_sum      = 0;
        Json::UInt seq_validity_sum = 0;
        
        // ... ensure mandatory fields ...
        const auto seq_job_tube      = GetJSONObject(a_payload, "tube"     , Json::ValueType::stringValue, /* a_default */ nullptr, seq_iomkmp).asString();
        const auto seq_job_ttr       = GetJSONObject(a_payload, "ttr"      , Json::ValueType::uintValue  , /* a_default */ 0, seq_iomkmp).asUInt();
        const auto seq_job_validity  = GetJSONObject(a_payload, "validity" , Json::ValueType::uintValue  , /* a_default */ 0, seq_iomkmp).asUInt();
        const auto seq_jobs          = GetJSONObject(a_payload, "jobs"     , Json::ValueType::arrayValue , /* a_default */ nullptr, seq_iomkmp);
        for ( Json::ArrayIndex idx = 0 ; idx < seq_jobs.size() ; ++idx ) {
            seq_ttr_sum      += GetJSONObject(seq_jobs[idx], "ttr"     , Json::ValueType::uintValue, &activity_config_.ttr_).asUInt();
            seq_validity_sum += GetJSONObject(seq_jobs[idx], "validity", Json::ValueType::uintValue, &activity_config_.validity_).asUInt();
        }
        
        // ... if sequence 'ttr' was ...
        if ( 0 == seq_job_ttr ) {
            // ... NOT provided, set as the sum of it's activities 'ttr' values ...
            seq_ttr = seq_ttr_sum;
            SEQUENCER_LOG_SEQUENCE(CC_JOB_LOG_LEVEL_WRN, a_sequence, CC_JOB_LOG_STEP_IN, "TTR not provided, setting " UINT64_FMT, static_cast<uint64_t>(seq_ttr_sum));
        } else if ( seq_job_ttr < seq_ttr_sum ) {
            // ... provided, BUT 'ttr' value is lower than the sum of it's activities 'ttr' values ...
            throw sequencer::BadRequestException(tracking,
                                                 ( "Provided sequence 'ttr' value ( " +
                                                        std::to_string(seq_job_ttr) +
                                                    " ) is lower that the sum of it's activities 'ttr' value ( "
                                                        + std::to_string(seq_ttr_sum) +
                                                  " )!"
                                                 ).c_str()
            );
        }
        // ... if sequence 'validity' was ...
        if ( 0 == seq_job_validity ) {
            // ... NOT provided, set as the sum of it's activities 'validity' values ...
            seq_validity = seq_validity_sum;
            SEQUENCER_LOG_SEQUENCE(CC_JOB_LOG_LEVEL_WRN, a_sequence, CC_JOB_LOG_STEP_IN, "Validity not provided, setting " UINT64_FMT, static_cast<uint64_t>(seq_validity));
        }else if ( seq_job_validity < seq_validity_sum ) {
            // ... provided, BUT 'validity' value is lower than the sum of it's activities 'validity' values ...
            throw sequencer::BadRequestException(tracking,
                                                 ( "Provided sequence 'validity' value ( " +
                                                        std::to_string(seq_job_validity) +
                                                    " ) is lower that the sum of it's activities 'validity' value ( "
                                                        + std::to_string(seq_validity_sum) +
                                                  " )!"
                                                 ).c_str()
            );
        }

    } catch (const ev::Exception& a_ev_exception) {
        throw sequencer::JSONValidationException(tracking, a_ev_exception.what());
    }
    
    
    // ... now register sequence ...
    CC_WARNING_TODO("CJS: validate payload");
    
    std::stringstream ss; ss.clear(); ss.str("");
    Json::FastWriter  jw; jw.omitEndingLineFeed();
    
    // ... js.register_sequence (pid INTEGER, cid INTEGER, bjid INTEGER, rjid TEXT, rcid TEXT, payload JSONB, activities JSONB, ttr INTEGER, validity INTEGER) ...
    ss << "SELECT * FROM js.register_sequence(";
    ss <<     config_.pid() << ',' << a_sequence.cid() << ',' << a_sequence.bjid();
    ss <<     ',' <<  "'" << a_sequence.rjid() << "'" << ',' << "'" << a_sequence.rcid() << "'";
    ss <<     ",'" << ::ev::postgresql::Request::SQLEscape(jw.write(a_payload)) << "'";
    ss <<     ",'" << ::ev::postgresql::Request::SQLEscape(jw.write(a_payload["jobs"])) << "'";
    ss <<     ',' << seq_ttr << ',' << seq_validity;
    ss << ");";
    
    //
    Json::Value activity = Json::Value::null;
    size_t      count    = 0;
    
    // ... register @ DB ...
    ExecuteQueryAndWait(/* a_tracking         */  tracking,
                        /* a_query            */ ss.str(), /* a_expected */ ExecStatusType::PGRES_TUPLES_OK,
                        /* a_success_callback */
                            [&count, &activity] (const Json::Value& a_value) {
                            count    = static_cast<size_t>(a_value.size());
                            activity = a_value[0];
                        }
    );
    
    // ... register sequence id from DB ...
    a_sequence.Bind(/* a_id    */ GetJSONObject(activity, "sid"     , Json::ValueType::intValue   , /* a_default */ nullptr).asString(),
                    /* a_count */ count
    );
    
    // ... log ...
    SEQUENCER_LOG_SEQUENCE(CC_JOB_LOG_LEVEL_INF, a_sequence, CC_JOB_LOG_STEP_POSGRESQL, "Registered with ID %s, " SIZET_FMT " %s",
                           a_sequence.did().c_str(),
                           a_sequence.count(), ( a_sequence.count() == 1 ? "actitity" : "activities" )
    );

    const auto did      = GetJSONObject(activity, "id"      , Json::ValueType::intValue   , /* a_default */ nullptr).asString();
    const auto job      = GetJSONObject(activity, "job"     , Json::ValueType::objectValue, /* a_default */ nullptr);
    
//    CC_WARNING_TODO("CJS: do we need this variable - used for validate 'tube' field existence ?");
//    const auto tube     = GetJSONObject(job     , "tube"    , Json::ValueType::stringValue, /* a_default */ nullptr).asString();
    
    const auto validity = GetJSONObject(job     , "validity", Json::ValueType::uintValue   , &activity_config_.validity_).asUInt();
    const auto ttr      = GetJSONObject(job     , "ttr"     , Json::ValueType::uintValue   , &activity_config_.ttr_).asUInt();
    
    // ... return first activity properties ...
    return sequencer::Activity(a_sequence, /* a_id */ did, /* a_index */ 0, /* a_attempt */ 0).Bind(sequencer::Status::Pending, validity, ttr, activity);
}

/**
 * @brief Cancel a sequence based on a running activity.
 *
 * @param a_activity Running activity info.
 * @param a_response Activity response.
 */
void casper::job::Sequencer::CancelSequence (const casper::job::sequencer::Activity& a_activity,
                                             const Json::Value& a_response)
{
    CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);

    
    // ... log ...
    SEQUENCER_LOG_SEQUENCE(CC_JOB_LOG_LEVEL_INF, a_activity.sequence(), "STEP", "Cancelling for ID %s", \
                           a_activity.sequence().did().c_str()
    );
    
    //
    // ⚠️ Since we don't have ( nor do we want to ) any context about the running activity,
    //    we are letting it run until it's finished and only mark as cancelled at the database
    //    to stop any other activity in the sequence to run.
    //
    std::stringstream ss; ss.clear(); ss.str("");
    Json::FastWriter  jw; jw.omitEndingLineFeed();
    
    // ... js.cancel_sequence (id INTEGER, status js.status, response JSONB) ...
    ss << "SELECT * FROM js.cancel_sequence(";
    ss <<   a_activity.sequence().did();
    ss <<   ",'" << ::ev::postgresql::Request::SQLEscape(jw.write(a_response)) << "'";
    ss << ");";
 
    double rtt = -1.0;
    
    // ... register @ DB ...
    ExecuteQueryAndWait(/* a_tracking */ SEQUENCER_TRACK_CALL(a_activity.sequence().bjid(), "CANCELLING JOB SEQUENCE"),
                        /* a_query    */ ss.str(), /* a_expected */ ExecStatusType::PGRES_TUPLES_OK,
                        /* a_success_callback */
                        [&rtt] (const Json::Value& a_value) {
                            // ... array with one element is expected ...
                            rtt = a_value[0]["rtt"].asDouble() * 1000;
                        }
    );
    
    // ... log ...
    SEQUENCER_LOG_SEQUENCE(CC_JOB_LOG_LEVEL_INF, a_activity.sequence(), "STEP", "Cancelled for ID %s", \
                           a_activity.sequence().did().c_str()
    );

    // ... copy sequence info, once cancelled / untracked it's relased and it's reference is no longer válid ...
    const sequencer::Sequence sequence = sequencer::Sequence(a_activity.sequence());
    
    // ... cancel activity ...
    CancelActivity(a_activity, a_response);
    
    ss.clear(); ss.str("");
    ss << sequencer::Status::Cancelled;
    
    // ... log sequence 'rtt' ...
    SEQUENCER_LOG_SEQUENCE(CC_JOB_LOG_LEVEL_INF, sequence, CC_JOB_LOG_STEP_RTT, DOUBLE_FMT_D(0) "ms",
                            rtt
    );

    // ... log sequence 'response' ...
    SEQUENCER_LOG_SEQUENCE(CC_JOB_LOG_LEVEL_INF, sequence, CC_JOB_LOG_STEP_OUT, "Response: " CC_JOB_LOG_COLOR(ORANGE) "%s" CC_LOGS_LOGGER_RESET_ATTRS,
                           jfw_.write(a_response).c_str()
    );
    
    // ... log sequence 'status' ...
    SEQUENCER_LOG_SEQUENCE(CC_JOB_LOG_LEVEL_INF, sequence, CC_JOB_LOG_STEP_STATUS, CC_JOB_LOG_COLOR(ORANGE) "%s" CC_LOGS_LOGGER_RESET_ATTRS,
                           ss.str().c_str()
    );
            
    // ... log job 'status' ..
    SEQUENCER_LOG_JOB(CC_JOB_LOG_LEVEL_INF, sequence.bjid(), CC_JOB_LOG_STEP_OUT, CC_JOB_LOG_COLOR(ORANGE) "%s" CC_LOGS_LOGGER_RESET_ATTRS,
                      ss.str().c_str()
    );
}


/**
 * @brief Call when the 'final' activity was performed so we close the sequence.
 *
 * @param a_activity Activity info.
 * @param a_response Activity response.
 * @param o_rtt      Activity RTT in milliseconds.
 */
void casper::job::Sequencer::FinalizeSequence (const casper::job::sequencer::Activity& a_activity,
                                               const Json::Value& a_response,
                                               double& o_rtt)
{
    CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);
    
    const auto& sequence = a_activity.sequence();

    // ... log ...
    SEQUENCER_LOG_SEQUENCE(CC_JOB_LOG_LEVEL_INF, sequence, "STEP", "Finalizing ( " SIZET_FMT " / " SIZET_FMT " %s )", \
                           ( a_activity.index() + 1 ), sequence.count(), ( sequence.count() == 1 ? "actitity" : "activities" )
    );

    std::stringstream ss; ss.clear(); ss.str("");
    Json::FastWriter  jw; jw.omitEndingLineFeed();
    
    // ... js.finalize_sequence (id INTEGER, status js.status, response JSONB) ...
    ss << "SELECT * FROM js.finalize_sequence(";
    ss <<   sequence.did() << ",'" << a_activity.status() << "'";
    ss <<   ",'" << ::ev::postgresql::Request::SQLEscape(jw.write(a_response)) << "'";
    ss << ");";
    
    o_rtt = 0;
    
    // ... register @ DB ...
    ExecuteQueryAndWait(/* a_tracking */ SEQUENCER_TRACK_CALL(sequence.bjid(), "FINALIZING JOB SEQUENCE"),
                        /* a_query    */ ss.str(), /* a_expected */ ExecStatusType::PGRES_TUPLES_OK,
                        /* a_success_callback */
                        [&o_rtt] (const Json::Value& a_value) {
                            // ... array with one element is expected ...
                            o_rtt = a_value[0]["rtt"].asDouble() * 1000;
                        }
    );
    
    // ... log ...
    SEQUENCER_LOG_SEQUENCE(CC_JOB_LOG_LEVEL_INF, sequence, CC_JOB_LOG_STEP_STEP, "Finalized ( " SIZET_FMT " / " SIZET_FMT " %s )",
                          ( a_activity.index() + 1 ), sequence.count(), ( sequence.count() == 1 ? "actitity" : "activities" )
    );
    
    ss.clear(); ss.str("");
    ss << a_activity.status();

    
    // ... pick log color  ...
    const char* response_color;
    const char* status_color;
    if ( sequencer::Status::Done == a_activity.status() ) {
        response_color = CC_JOB_LOG_COLOR(GREEN);
        status_color   = CC_JOB_LOG_COLOR(LIGHT_GREEN);
    } else if ( sequencer::Status::Failed == a_activity.status() ) {
        response_color = CC_JOB_LOG_COLOR(RED);
        status_color   = CC_JOB_LOG_COLOR(LIGHT_RED);
    } else {
        response_color = CC_JOB_LOG_COLOR(ORANGE);
        status_color   = CC_JOB_LOG_COLOR(ORANGE);
    }

    // ... log sequence 'rtt' ...
    SEQUENCER_LOG_SEQUENCE(CC_JOB_LOG_LEVEL_INF, sequence, CC_JOB_LOG_STEP_RTT, DOUBLE_FMT_D(0) "ms",
                            o_rtt
    );

    // ... log sequence 'response' ...
    SEQUENCER_LOG_SEQUENCE(CC_JOB_LOG_LEVEL_INF, sequence, CC_JOB_LOG_STEP_OUT, "Response: %s%s" CC_LOGS_LOGGER_RESET_ATTRS,
                           response_color, jfw_.write(a_response).c_str()
    );
    
    // ... log sequence 'status' ...
    SEQUENCER_LOG_SEQUENCE(CC_JOB_LOG_LEVEL_INF, sequence, CC_JOB_LOG_STEP_STATUS, "%s%s" CC_LOGS_LOGGER_RESET_ATTRS,
                           status_color, ss.str().c_str()
    );
            
    // ... log job 'status' ..
    SEQUENCER_LOG_JOB(CC_JOB_LOG_LEVEL_INF, sequence.bjid(), CC_JOB_LOG_STEP_OUT, "%s%s" CC_LOGS_LOGGER_RESET_ATTRS,
                      status_color, ss.str().c_str()
    );
}

#ifdef __APPLE__
#pragma mark -
#endif

/**
 * @brief Launch an activity ( a.k.a inner job ).
 *
 * @param a_tracking Call tracking proposes.
 * @param a_activity Activity info.
 * @param a_at_run   True when called from 'run' method.
 *
 * @return HTTP status code.
 */
uint16_t casper::job::Sequencer::LaunchActivity (const casper::job::sequencer::Tracking& a_tracking,
                                                 casper::job::sequencer::Activity& a_activity, const bool a_at_run)
{
    CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);
    
    const auto& sequence = a_activity.sequence();

    // ... log ...
    SEQUENCER_LOG_ACTIVITY(CC_JOB_LOG_LEVEL_VBS, a_activity, CC_JOB_LOG_STEP_STEP,
                           "%s", "Launching");
        
    const std::string seq_id_key = config_.service_id() + ":jobs:sequential_id";

    typedef struct {
        std::string tube_;
        int64_t     expires_in_;
        uint32_t    ttr_;
        uint64_t    id_;
        std::string key_;
        std::string channel_;
        uint16_t    sc_;
    } ActivityJob;
    
    ActivityJob job_defs;
    
    try {
        
        const auto job       = GetJSONObject(a_activity.payload(), "job"     , Json::ValueType::objectValue, /* a_default */ nullptr);
        job_defs.tube_       = GetJSONObject(job                 , "tube"    , Json::ValueType::stringValue, /* a_default */ nullptr).asString();
        job_defs.expires_in_ = GetJSONObject(job                 , "validity", Json::ValueType::intValue   , &activity_config_.validity_).asUInt();
        job_defs.ttr_        = GetJSONObject(job                 , "ttr"     , Json::ValueType::intValue   , &activity_config_.ttr_).asUInt();
        
    } catch (const ev::Exception& a_ev_exception) {
        throw sequencer::JSONValidationException(a_tracking, a_ev_exception.what());
    }
    
    job_defs.id_         = 0;
    job_defs.key_        = ( config_.service_id() + ":jobs:" + job_defs.tube_ + ':' );
    job_defs.channel_    = ( config_.service_id() + ':'      + job_defs.tube_ + ':' );
    job_defs.sc_         = 500;
    
    // ...
    osal::ConditionVariable cv;
    ExecuteOnMainThread([this, &cv, &job_defs, &seq_id_key, &a_tracking] () {

        NewTask([this, &seq_id_key] () -> ::ev::Object* {
            
            // ...  get new job id ...
            return new ::ev::redis::Request(loggable_data_, "INCR", {
                /* key   */ seq_id_key
            });
            
        })->Then([this, &job_defs] (::ev::Object* a_object) -> ::ev::Object* {
            
            //
            // INCR:
            //
            // - An integer reply is expected:
            //
            //  - the value of key after the increment
            //
            const ::ev::redis::Value& value = ::ev::redis::Reply::EnsureIntegerReply(a_object);
            
            job_defs.id_       = static_cast<uint64_t>(value.Integer());
            job_defs.key_     += std::to_string(job_defs.id_);
            job_defs.channel_ += std::to_string(job_defs.id_);
            
            // ... first, set queued status ...
            return new ::ev::redis::Request(loggable_data_, "HSET", {
                /* key   */ job_defs.key_,
                /* field */ "status", "{\"status\":\"queued\"}"
            });
            
        })->Then([this, &job_defs] (::ev::Object* a_object) -> ::ev::Object* {
            
            //
            // HSET:
            //
            // - An integer reply is expected:
            //
            //  - 1 if field is a new field in the hash and value was set.
            //  - 0 if field already exists in the hash and the value was updated.
            //
            (void)::ev::redis::Reply::EnsureIntegerReply(a_object);
            
            return new ::ev::redis::Request(loggable_data_, "EXPIRE", { job_defs.key_, std::to_string(job_defs.expires_in_) });
            
        })->Finally([&cv, &job_defs] (::ev::Object* a_object) {
            
            //
            // EXPIRE:
            //
            // Integer reply, specifically:
            // - 1 if the timeout was set.
            // - 0 if key does not exist or the timeout could not be set.
            //
            ::ev::redis::Reply::EnsureIntegerReply(a_object, 1);

            //
            // DONE
            //
            job_defs.sc_ = 200;
            
            // RELEASE job control
            cv.Wake();
            
        })->Catch([this, &cv, &job_defs, &a_tracking] (const ::ev::Exception& a_ev_exception) {
            
            job_defs.sc_ = 500;
            
            CC_WARNING_TODO("CJS: BETTER EXCEPTION REPORT - SEE, merge and fix ExecuteQueryAndWait");
            
            const std::string prefix = "rror while setting up REDIS job to tube " + job_defs.tube_;
            
            AppendError(/* a_type  */ "REDIS",
                        /* a_why   */ ( 'E'+ prefix + ": " + a_ev_exception.what() ).c_str(),
                        /* a_where */ a_tracking.function_.c_str(),
                        /* a_code */ ev::loop::beanstalkd::Job::k_exception_rc_
            );
            
            // RELEASE job control
            cv.Wake();
            
        });

    }, /* a_blocking */ false);

    // WAIT until job is submitted
    cv.Wait();
    
    //
    // CONTINUE OR ROLLBACK?
    //
    if ( 200 != job_defs.sc_ ) {
        // ... an error is already set ...
        return job_defs.sc_;
    }
    
    sequencer::Exception* exception = nullptr;
    
    //
    //  ... prepare, register and push activity job ...
    //
    try {
        // ... bind ids ...
        a_activity.Bind(/* rjnr_ */ job_defs.id_, /* a_rjid */ job_defs.key_, /* a_rcnm */ job_defs.tube_, /* a_rcid */ job_defs.channel_, /* a_new_attempt */ true);
        // ... grab job object ...
        const auto job = GetJSONObject(a_activity.payload(), "job" , Json::ValueType::objectValue, /* a_default */ nullptr);
        // .. first, copy payload ( so it can be patched ) ...
        Json::Value payload = job["payload"];
        // ... log ...
        CC_DEBUG_LOG_MSG("job", "Job #" INT64_FMT " ~= patching activity #" SIZET_FMT " - %s",
                           sequence.bjid(), ( a_activity.index() + 1 ), a_activity.rcid().c_str()
        );
        // ... debug only ..
        CC_DEBUG_LOG_MSG("job", "Job #" INT64_FMT " ~= before patch:\n%s",
                         sequence.bjid(), jsw_.write(payload).c_str()
        );
        // ... set or overwrite 'id' and 'tube' properties ...
        payload["id"]   = std::to_string(job_defs.id_);
        payload["tube"] = job_defs.tube_;
        if ( false == payload.isMember("ttr") ) {
            payload["ttr"] = a_activity.ttr();
        }
        if ( false == payload.isMember("validity") ) {
            payload["validity"] = job_defs.expires_in_;
        }
        // ... debug only ...
        CC_DEBUG_LOG_MSG("job", "Job #" INT64_FMT " ~= after patch:\n%s",
                         sequence.bjid(), jsw_.write(payload).c_str()
        );
        // ... log ...
        CC_DEBUG_LOG_MSG("job", "Job #" INT64_FMT " ~= patched activity #" SIZET_FMT " - %s",
                          sequence.bjid(), ( a_activity.index() + 1 ), a_activity.rcid().c_str()
        );
        // ... tmp track payload, ttr and validity ...
        a_activity.SetPayload(payload);
        a_activity.SetTTR(job_defs.ttr_);
        a_activity.SetValidity(job_defs.ttr_);
        // ... if required, evaluate all string fields as V8 expressions ...
        PatchActivity(a_tracking, a_activity);
        // ... now register activity attempt to launch @ db ...
        RegisterActivity(a_activity);
        // ... track activity ...
        TrackActivity(a_activity);
        // ... then, listen to REDIS job channel ...
        SubscribeActivity(a_activity);
        // ... now, push job ( send it to beanstalkd ) ...
        PushActivity(a_activity);
    } catch (const sequencer::V8ExpressionEvaluationException& a_v8eee) {
        exception = new sequencer::V8ExpressionEvaluationException(a_tracking, a_v8eee);
    } catch (...) {
        // ... recapture exception ...
        try {
            // ... first rethrow ...
            ::cc::Exception::Rethrow(/* a_unhandled */ true, __FILE__, __LINE__, __FUNCTION__);
        } catch (::cc::Exception& a_cc_exception) {
            // ... copy exception ...
            exception = new sequencer::Exception(a_tracking, 400, a_cc_exception.what());
        }
    }
    
    if ( nullptr != exception ) {
        // ... TODO ..
        CC_WARNING_TODO("CJS: TEST THIS CODE");
        // ... forget tmp payload ...
        a_activity.SetPayload(Json::Value::null);
        // ... and this activity ...
        UntrackActivity(a_activity); // ... ⚠️  a_activity STILL valid - it's the original one! ⚠️ ...
        // ... unsubscribe activity ...
        UnsubscribeActivity(a_activity);
        // ... log ...
        SEQUENCER_LOG_ACTIVITY(CC_JOB_LOG_LEVEL_ERR, a_activity, CC_JOB_LOG_STEP_ERROR,
                               "An error occurred while launching activity ~ %s",
                               exception->what()
        );
        // ... if at 'run' function ...
        if ( true == a_at_run ) {
            // ... copy exception ...
            const auto copy = sequencer::Exception(a_tracking, exception->code_, exception->what());
            // ... reset
            a_activity.Reset(sequencer::Status::Failed, /* a_payload */ Json::Value::null);
            // ... get rid of exception ...
            delete exception;
            exception = nullptr;
            // ... exceptions can be thrown here ...
            throw copy;
        } else {
            // ... track error ...
            AppendError(/* a_type  */ a_tracking.action_.c_str(),
                        /* a_why   */ exception->what(),
                        /* a_where */ a_tracking.function_.c_str(),
                        /* a_code */ ev::loop::beanstalkd::Job::k_exception_rc_
            );
            Json::Value errors = Json::Value::null;
            // ... override with errors serialization ...
            SetFailedResponse(/* a_code */ exception->code_, errors);
            // ... reset
            a_activity.Reset(sequencer::Status::Failed, /* a_payload */ errors);
            // ... get rid of exception ...
            delete exception;
            exception = nullptr;
            // ... just 'finalize' activity ( by setting failed status ) ...
            (void)ActivityReturned(a_tracking, a_activity, /* a_response */ nullptr);
        }
    }
    
    // ... ensure we won't leak exception ...
    CC_ASSERT(nullptr == exception);
    
    // ... reset ptr ...
    a_activity.SetPayload(Json::Value::null);
    
    // ... log ...
    SEQUENCER_LOG_ACTIVITY(CC_JOB_LOG_LEVEL_INF, a_activity, CC_JOB_LOG_STEP_STEP,
                           "Launched with REDIS channel ID %s", a_activity.rcid().c_str());
    
    // ... we're done ...
    return job_defs.sc_;
}

/**
 * @brief Call this method when an activity has a message to be relayed to the sequencer job.
 *
 * @param a_tracking Call tracking proposes.
 * @param a_activity Activity info.
 * @param a_message  JSON object to relay.
 *
 * @return HTTP status code.
*/
void casper::job::Sequencer::ActivityMessageRelay (const casper::job::sequencer::Tracking& /* a_tracking */,
                                                   const casper::job::sequencer::Activity& a_activity, const Json::Value& a_message)
{
    CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);
    
    const std::string src_channel_key = a_activity.rcid();
    const std::string dst_channel_key = a_activity.sequence().rcid();

    // ... log ...
    SEQUENCER_LOG_ACTIVITY(CC_JOB_LOG_LEVEL_DBG, a_activity, CC_JOB_LOG_STEP_RELAY,
                           CC_JOB_LOG_COLOR(YELLOW) "Relay message" CC_LOGS_LOGGER_RESET_ATTRS " from %s to %s, " CC_JOB_LOG_COLOR(DARK_GRAY) "%s" CC_LOGS_LOGGER_RESET_ATTRS,
                           src_channel_key.c_str(), dst_channel_key.c_str(), jfw_.write(a_message).c_str()
    );
        
    try {
        Relay(a_activity.sequence().bjid(), dst_channel_key, a_message);
    } catch (...) {
        try {
            ::cc::Exception::Rethrow(/* a_unhandled */ true, __FILE__, __LINE__, __FUNCTION__);
        } catch (const ::cc::Exception& a_cc_exception) {
            CC_WARNING_TODO("CJS: HANDLE EXCEPTION");
            // ... log ...
            SEQUENCER_LOG_ACTIVITY(CC_JOB_LOG_LEVEL_WRN, a_activity, CC_JOB_LOG_STEP_RELAY,
                                   CC_JOB_LOG_COLOR(RED) "Failed to relay message"
                                        CC_LOGS_LOGGER_RESET_ATTRS " from %s to %s, " CC_JOB_LOG_COLOR(DARK_GRAY) "%s" CC_LOGS_LOGGER_RESET_ATTRS,
                                   src_channel_key.c_str(), dst_channel_key.c_str(), a_cc_exception.what()
            );
        }
    }
}

/**
 * @brief Call this method when an activity has returned ( the next in sequence will be launched if needed ).
 *
 * @param a_tracking Call tracking proposes.
 * @param a_activity Activity info.
 * @param a_response Activity response.
 *
 * @return HTTP status code.
 */
void casper::job::Sequencer::ActivityReturned (const casper::job::sequencer::Tracking& a_tracking,
                                               const casper::job::sequencer::Activity& a_activity, const Json::Value* a_response)
{
    CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);
    
    // ... prepare next activity ...
    sequencer::Activity next = sequencer::Activity(/* a_sequence */ a_activity.sequence(), /* a_id */ a_activity.did(), /* a_index */ a_activity.index(), /* a_attempt */ 0);

    // ... finalize activity and pick next ( if any ) ...
    FinalizeActivity(a_activity, a_response, next);
    
    // ... copy activity ...
    const auto returning_activity = sequencer::Activity(a_activity);
    
    // ... untrack activity ...
    UntrackActivity(a_activity); // ... ⚠️ from now on a_activity is NOT valid ! ⚠️ ...
    
    // ... unsubscribe activity ...
    UnsubscribeActivity(returning_activity);
    
    // ... do we have another activity?
    if ( sequencer::Status::Pending == next.status() ) {
        // ... we're ready to next activity ...
        CC_ASSERT(next.index() != returning_activity.index());
        CC_ASSERT(0 != next.did().compare(returning_activity.did()));
        // ... launch activity ...
        LaunchActivity(a_tracking, next, /* a_at_run */ false);
    } else {

        // ... set final response ...
        const Json::Value* job_response;
        
        if ( nullptr != a_response ) {
            // ... a valid response was provided ...
            job_response = a_response;
        } else {
            CC_WARNING_TODO("CJS: review IF");
            // ... a critical error occurred?
            if ( sequencer::Status::Failed == returning_activity.status() ) {
                // ... activity payload must the the error do display ...
                job_response = &returning_activity.payload();
            } else if ( sequencer::Status::Done == next.status() ) { // ... we're done?
                // ... we're done ...
                job_response = a_response;
            } else {
                // ... critical error ...
                job_response = &next.payload();
            }
        }
               
        // ... job_response can't be nullptr!
        CC_ASSERT(nullptr != job_response);
        CC_ASSERT(false == job_response->isNull() && true == job_response->isMember("status"));
        
        double rtt = 0;
        
        // ... finalize sequence ...
        FinalizeSequence(returning_activity, *job_response, rtt);
        
        // ... finish job ...
        FinalizeJob(returning_activity.sequence(), *job_response);
    }
}

/**
 * @brief Register an attempt to launch an activity job.
 *
 * @param a_activity Activity info.
 */
void casper::job::Sequencer::RegisterActivity (const casper::job::sequencer::Activity& a_activity)
{
    CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);
    
    // ... log ...
    SEQUENCER_LOG_ACTIVITY(CC_JOB_LOG_LEVEL_VBS, a_activity, CC_JOB_LOG_STEP_POSGRESQL,
                           "%s", "Registering");

    //
    // FORMAT:
    //
    // ...
    //         "attempts": [{
    //             "launched_at": , <string>       - NowISO8601WithTZ
    //             "payload":     , <json>         - sent payload ( original if index == 0 or after patch ( v8 ) when index > 0 )
    //         }],
    // ...
    //
    
    std::stringstream ss; ss.clear(); ss.str("");
    Json::FastWriter  jw; jw.omitEndingLineFeed();
    
    Json::Value data = Json::Value(Json::ValueType::objectValue);
    
    data["launched_at"] = cc::UTCTime::NowISO8601WithTZ();
    data["payload"    ] = a_activity.payload();
    data["bjid"       ] = a_activity.sequence().bjid();
    data["rjid"       ] = a_activity.rjid();
    data["rcid"       ] = a_activity.rcid();
      
    // ... js.register_activity (sid INTEGER, id INTEGER, bjid INTEGER, rjid TEXT, rcid TEXT, attempt INTEGER, payload JSONB, status js.status); ...
    ss << "SELECT * FROM js.register_activity(";
    ss <<   a_activity.sequence().did() << ',' << a_activity.did();
    ss <<  ',' << a_activity.sequence().bjid() << ",'" << a_activity.rjid() << "','" << a_activity.rcid() << "'";
    ss <<  ',' << a_activity.attempt();
    ss <<   ",'" << ::ev::postgresql::Request::SQLEscape(jw.write(data)) << "'";
    ss <<   ",'" << sequencer::Status::InProgress << "'";
    ss << ");";
      
    // ... execute query ...
    ExecuteQueryAndWait(/* a_tracking */ SEQUENCER_TRACK_CALL(a_activity.sequence().bjid(), "REGISTERING ACTIVITY"),
                        /* a_query    */ ss.str(), /* a_expected */ ExecStatusType::PGRES_TUPLES_OK
    );
    
    // ... log ...
    SEQUENCER_LOG_ACTIVITY(CC_JOB_LOG_LEVEL_INF, a_activity, CC_JOB_LOG_STEP_POSGRESQL,
                           "Registered with ID %s",
                           a_activity.did().c_str()
    );
}


#ifdef __APPLE__
#pragma mark -
#endif

/**
 * @brief Subscribe to an activity REDIS channel.
 *
 * @param a_activity Activity info.
 */
void casper::job::Sequencer::SubscribeActivity (const casper::job::sequencer::Activity& a_activity)
{
    CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);
    
    // ... log ...
    SEQUENCER_LOG_ACTIVITY(CC_JOB_LOG_LEVEL_VBS, a_activity, CC_JOB_LOG_STEP_REDIS,
                           "Subscribing to channel '%s'",
                           a_activity.rcid().c_str()
    );

    osal::ConditionVariable cv;
    
    ExecuteOnMainThread([this, &a_activity, &cv] () {
        
        ::ev::redis::subscriptions::Manager::GetInstance().SubscribeChannels({ a_activity.rcid()},
            [this, &a_activity, &cv](const std::string& a_id, const ::ev::redis::subscriptions::Manager::Status& a_status) -> EV_REDIS_SUBSCRIPTIONS_DATA_POST_NOTIFY_CALLBACK {
                // ... subscribed?
                if ( ::ev::redis::subscriptions::Manager::Status::Subscribed == a_status ) {
                    // ... log ...
                    SEQUENCER_LOG_ACTIVITY(CC_JOB_LOG_LEVEL_INF, a_activity, CC_JOB_LOG_STEP_REDIS,
                                           "Subscribed to channel '%s'",
                                           a_activity.rcid().c_str()
                    );
                    // ... we're done ...
                    cv.Wake();
                }
                return nullptr;
            },
            std::bind(&casper::job::Sequencer::OnActivityMessageReceived, this, std::placeholders::_1, std::placeholders::_2),
            this
         );
        
    }, /* a_blocking */ false);
    
    cv.Wait();
}

/**
 * @brief Called by REDIS subscriptions manager.
 *
 * @param a_id      REDIS channel id.
 * @param a_message Channel's message.
 */
EV_REDIS_SUBSCRIPTIONS_DATA_POST_NOTIFY_CALLBACK casper::job::Sequencer::OnActivityMessageReceived (const std::string& a_id, const std::string& a_message)
{
    
    CC_WARNING_TODO("CJS: HANDLE CANCELLATION MESSAGE!!");
    
    CC_DEBUG_FAIL_IF_NOT_AT_MAIN_THREAD();
    
    ScheduleCallbackOnLooperThread(/* a_id */ "sequencer-activity-message-callback",
                                   /* a_callback */ [this, a_id, a_message](const std::string& /* a_id */) {
        
        sequencer::Sequence*  sequence  = nullptr;
            sequencer::Exception* exception = nullptr;

            CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);

            // ... expecting message?
            const auto activity_it = running_activities_.find(a_id);
            if ( running_activities_.end() == activity_it ) {
                // ... log ...
                CC_DEBUG_LOG_MSG("job", "Job #" INT64_FMT " ~= '%s': %s",
                                 static_cast<uint64_t>(0), a_id.c_str(), "ignored"
                );
                // ... not expected, we're done ...
                return;
            }
            
            try {
                
                Json::Value  object;
                
                // ... parse JSON message ...
                AsJSON(a_message, object);
                
                // ... check this inner job status ...
                const std::string status = object.get("status", "").asCString();
                CC_DEBUG_LOG_MSG("job", "Job #" INT64_FMT " ~= '%s': status is %s",
                                 activity_it->second->sequence().bjid(), a_id.c_str(), status.c_str()
                );
                
                // ... interest in this status ( completed, failed or cancelled ) ?
                const auto m_it = s_irj_teminal_status_map_.find(status);
                if ( s_irj_teminal_status_map_.end() == m_it ) {
                    //
                    // ... relay 'in-progress' messages ...
                    // ... ( because we may have more activities to run ) ...
                    if ( 0 == status.compare("in-progress") ) {
                        ActivityMessageRelay(SEQUENCER_TRACK_CALL(activity_it->second->sequence().bjid(), "ACTIVITY MESSAGE RELAY"),
                                             *activity_it->second, object
                        );
                    }
                    // ... no moe interest, we're done
                    return;
                }
                
                // ... update activity statuus ...
                activity_it->second->SetStatus(m_it->second);
                
                // ... copy sequence info - for try catch ...
                sequence = new sequencer::Sequence(activity_it->second->sequence());
                
                //
                // ... we're interested:
                // ... ( completed, failed or cancelled )
                //
                // ... - we've got all required data to finalize this inner job
                // ... - we can launch the next inner job ( if required )
                //
                ActivityReturned(SEQUENCER_TRACK_CALL(activity_it->second->sequence().bjid(), "RETURNING ACTIVITY"),
                                 *activity_it->second, &object
                );
                
                // ... ⚠️ from now on a_activity is NOT valid ! ⚠️ ...
                
            } catch (const sequencer::JumpErrorAlreadySet& a_jp_exception) {

                // ... copy exception ...
                exception = new sequencer::JumpErrorAlreadySet(a_jp_exception);
                // ... log it ...
                CC_JOB_LOG_TRACE(CC_JOB_LOG_LEVEL_DBG, "Job #" INT64_FMT " ~= ERROR JUMP =~\n\nORIGIN: %s:%d\nACTION: %s\n\%s\n",
                                 a_jp_exception.tracking_.bjid_,
                                 a_jp_exception.tracking_.function_.c_str(), a_jp_exception.tracking_.line_,
                                 a_jp_exception.tracking_.action_.c_str(),
                                 a_jp_exception.what()
                );
            
            } catch (const sequencer::Exception& a_sq_exception) {
                // ... copy exception ...
                exception = new sequencer::Exception(a_sq_exception);
                // ... log it ...
                CC_JOB_LOG_TRACE(CC_JOB_LOG_LEVEL_DBG, "Job #" INT64_FMT "'%s': %s",
                                 sequence->bjid(), a_id.c_str(), a_sq_exception.what()
                );
            } catch (const ::cc::Exception& a_cc_exception) {
                // ... copy exception ...
                exception = new sequencer::Exception(SEQUENCER_TRACK_CALL(activity_it->second->sequence().bjid(), "CC EXCEPTION CAUGHT"), /* a_code */ 500, a_cc_exception.what());
                // ... log it ...
                CC_JOB_LOG_TRACE(CC_JOB_LOG_LEVEL_DBG, "Job #" INT64_FMT "'%s': %s",
                                 sequence->bjid(), a_id.c_str(), a_cc_exception.what()
                );
            } catch (...) {
                try {
                    ::cc::Exception::Rethrow(/* a_unhandled */ true, __FILE__, __LINE__, __FUNCTION__);
                } catch (const ::cc::Exception& a_cc_exception) {
                    // ... copy exception ...
                    exception = new sequencer::Exception(SEQUENCER_TRACK_CALL(activity_it->second->sequence().bjid(), "GENERIC CC EXCEPTION CAUGHT"), /* a_code */ 500, a_cc_exception.what());
                    // ... log it ...
                    CC_JOB_LOG_TRACE(CC_JOB_LOG_LEVEL_DBG, "Job #" INT64_FMT "'%s': %s",
                                     sequence->bjid(), a_id.c_str(), a_cc_exception.what()
                    );
                }
            }

            // ... can't accept if no sequence is set ...
            CC_ASSERT(nullptr != sequence);

            // ... if an exception was thrown ...
            if ( nullptr != exception ) {
                //
                Json::Value response = Json::Value::null;
                // ... build response ..
                SetFailedResponse(exception->code_, response);
                // ... notify 'job finished' ...
                FinalizeJob(*sequence, response);
                // ... cleanup ...
                delete sequence;
                delete exception;
            } else {
                // ... cleanup ...
                delete sequence;
            }
        
    });
    
    // ... we're done ..
    return nullptr;
}

/**
 * @brief Unsubscribe to an activity REDIS channel.
 *
 * @param a_activity Activity info.
 */
void casper::job::Sequencer::UnsubscribeActivity (const casper::job::sequencer::Activity& a_activity)
{
    CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);
    
    // ... log ...
    SEQUENCER_LOG_ACTIVITY(CC_JOB_LOG_LEVEL_VBS, a_activity, CC_JOB_LOG_STEP_REDIS,
                           "Unsubscribing from channel '%s'",
                           a_activity.rcid().c_str()
    );

    osal::ConditionVariable cv;
    
    ExecuteOnMainThread([this, &a_activity, &cv] () {
        
        ::ev::redis::subscriptions::Manager::GetInstance().UnsubscribeChannels({ a_activity.rcid() },
            [this,  &a_activity, &cv](const std::string& a_id, const ::ev::redis::subscriptions::Manager::Status& a_status) -> EV_REDIS_SUBSCRIPTIONS_DATA_POST_NOTIFY_CALLBACK {
                if ( ::ev::redis::subscriptions::Manager::Status::Unsubscribed == a_status ) {
                    // ... log ...
                    SEQUENCER_LOG_ACTIVITY(CC_JOB_LOG_LEVEL_INF, a_activity, CC_JOB_LOG_STEP_REDIS,
                                           "Unsubscribed from channel '%s'",
                                           a_activity.rcid().c_str()
                    );
                    // ... we're done ...
                    cv.Wake();
                }
                return nullptr;
            },
            this
         );
        
    }, /* a_blocking */ false);
    
    cv.Wait();
}

#ifdef __APPLE__
#pragma mark -
#endif

/**
 * @brief Push an activity to BEANSTALKD queue.
 *
 * @param a_activity Activity info.
 */
    void casper::job::Sequencer::PushActivity (const casper::job::sequencer::Activity& a_activity)
{
    CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);
    
    // ... log ...
    SEQUENCER_LOG_ACTIVITY(CC_JOB_LOG_LEVEL_VBS, a_activity, CC_JOB_LOG_STEP_BEANSTALK,
                           "%s", "Pushing to beanstalkd");
    
    Json::FastWriter jw; jw.omitEndingLineFeed();
    
    // ... submit job to beanstalkd queue ...
    PushJob(a_activity.payload()["tube"].asString(), jw.write(a_activity.payload()), a_activity.ttr());
    
    // ... log ...
    SEQUENCER_LOG_ACTIVITY(CC_JOB_LOG_LEVEL_INF, a_activity, CC_JOB_LOG_STEP_BEANSTALK,
                           "%s", "Pushed to beanstalkd");
}

/**
 * @brief Register an activity finalization.
 *
 * @param a_activity Activity info.
 * @param a_response Activity response.
 * @param o_next     Pre-filled next activity info ( payload, ttr and status to be set here, if there is another activity to load ).
 */
void casper::job::Sequencer::FinalizeActivity (const casper::job::sequencer::Activity& a_activity, const Json::Value* a_response,
                                               casper::job::sequencer::Activity& o_next)
{
    CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);
    
    //
    // FORMAT:
    //
    // ...
    //         "attempts": [{
    //             "finished_at": , <string>       - NowISO8601WithTZ
    //             "rtt":         , <unsigned int> - number of seconds that took to launch, execute and register job
    //             "response":    , <json>         - received response
    //         }],
    // ...
    //
    CC_WARNING_TODO("CJS: implement retry mechanism beware of status meaning");
    
    std::stringstream ss; ss.clear(); ss.str("");
    Json::FastWriter  jw; jw.omitEndingLineFeed();
    
    Json::Value attempt = Json::Value(Json::ValueType::objectValue);
    
    //
    // ... ensure next activity is not valid yet ...
    //
    o_next.SetStatus(casper::job::sequencer::Status::NotSet);
    
    //
    // NOTICE: rtt will be calculated and set unpon js.finalize_activity execution ...
    //
    attempt["finished_at"] = cc::UTCTime::NowISO8601WithTZ();
    // ... if a_response was provided ...
    if ( nullptr != a_response ) {
        // ... copy response ...
        attempt["response"] = *a_response;
    } else {
        // ... else, payload MUST be already set, use it ...
        attempt["response"] = a_activity.payload();
    }
    
    // ... js.finalize_activity (sid INTEGER, id INTEGER, attempt INTEGER, payload JSONB, response JSONB, status js.status) ...
    ss << "SELECT * FROM js.finalize_activity(";
    ss <<   a_activity.sequence().did() << ',' << a_activity.did();
    ss <<  ',' << a_activity.attempt() << ",'" << ::ev::postgresql::Request::SQLEscape(jw.write(attempt)) << "'";
    ss <<  ",'" << ::ev::postgresql::Request::SQLEscape(jw.write(attempt["response"])) << "'";
    ss <<  ",'" << a_activity.status() << "'";
    ss << ");";
  
    // ... log ...
    SEQUENCER_LOG_ACTIVITY(CC_JOB_LOG_LEVEL_VBS, a_activity, CC_JOB_LOG_STEP_POSGRESQL,
                           "%s", "Registering finalization");
    
    double rtt = -1.0;
    
    // ... execute query ...
    ExecuteQueryAndWait(/* a_tracking         */ SEQUENCER_TRACK_CALL(a_activity.sequence().bjid(), "FINALIZING ACTIVITY"),
                        /* a_query            */ ss.str(), /* a_expect   */ ExecStatusType::PGRES_TUPLES_OK,
                        /* a_success_callback */
                        [&o_next, &rtt] (const Json::Value& a_value) {
                            // ... array is expected ...
                            if ( a_value.size() > 0 ) {
                                // ... ⚠️ we're returning the last activity rtt in the next activity ...
                                rtt = a_value[0]["rtt"].asDouble() * 1000;
                                // ... if ID is not rull then we've a 'next' activity ...
                                const Json::Value& next = a_value[0];
                                if ( false == next["id"].isNull() ) {
                                    o_next.Reset(sequencer::Status::Pending, /* a_payload */ next);
                                    o_next.SetIndex(static_cast<ssize_t>(next["index"].asUInt()));
                                    o_next.SetDID(next["id"].asString());
                                }
                            }
                        }
    );
        
    // ... log ...
    SEQUENCER_LOG_ACTIVITY(CC_JOB_LOG_LEVEL_INF, a_activity, CC_JOB_LOG_STEP_POSGRESQL,
                            "%s", "Finalization registered");
            
    // ... log activity 'rtt' ...
    SEQUENCER_LOG_ACTIVITY(CC_JOB_LOG_LEVEL_INF, a_activity, CC_JOB_LOG_STEP_RTT, DOUBLE_FMT_D(0) "ms",
                            rtt
    );

    // ... log activity 'status' ...
    ss.clear(); ss.str("") ; ss << a_activity.status();
    SEQUENCER_LOG_ACTIVITY(CC_JOB_LOG_LEVEL_INF, a_activity, CC_JOB_LOG_STEP_STATUS,
                           "%s",
                            ss.str().c_str()
    );
    SEQUENCER_LOG_ACTIVITY(CC_JOB_LOG_LEVEL_INF, a_activity, CC_JOB_LOG_STEP_STEP,
                           "Response: " CC_JOB_LOG_COLOR(DARK_GRAY) "%s" CC_LOGS_LOGGER_RESET_ATTRS,
                           ( nullptr != a_response ? jfw_.write(*a_response).c_str() : "<empty>" )
    );
    
    // ... based on a_response, set activity status ...
    if ( nullptr == a_response ) {
        // ... first activity launch, or returing failed ...
        o_next.SetStatus(casper::job::sequencer::Status::Done);
    } else {
        // ... next activity available?
        if ( casper::job::sequencer::Status::Pending == o_next.status() ) {
            // ... yes, but first check if previous activity succeeded ...
            const Json::Value status = (*a_response).get("status", "");
            const auto         m_it  = s_irj_teminal_status_map_.find(status.asCString());
            if ( s_irj_teminal_status_map_.end() == m_it ) {
                Json::Value response = Json::Value::null;
                // ... set standard 'failed' response ...
                SetFailedResponse(/* a_code */ 404, Json::Value("Invalid status '" + std::string(status.asCString()) + "'"), response);
                // ... invalid status - set internal error ...
                o_next.Reset(sequencer::Status::Failed, /* a_payload */ response);
            }
        } else {
            // ... no, we're done ...
            o_next.SetStatus(sequencer::Status::Done);
        }
    }

    // ... can't accept NotSet status ...
    CC_ASSERT(casper::job::sequencer::Status::NotSet != o_next.status());
}

/**
 * @brief Cancel a running activity.
 *
 * @param a_activity Activity info.
 * @param a_response Activity response.
 */
void casper::job::Sequencer::CancelActivity (const casper::job::sequencer::Activity& a_activity, const Json::Value& a_response)
{
    CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);
    
    // ... copy activity info, once cancelled / untracked it's relased and it's reference is no longer válid ...
    const sequencer::Activity activity = sequencer::Activity(a_activity);

    // ... untrack activity ...
    UntrackActivity(a_activity); // ... ⚠️ from now on a_activity is NOT valid ! ⚠️ ...

    // ... unsubscribe activity ...
    UnsubscribeActivity(activity);

    // ... signal activity's job to cancel ...
    Cancel(activity.sequence().bjid(), activity.rcid());
}

#ifdef __APPLE__
#pragma mark -
#endif

/**
 * @brief Track an activity.
 *
 * @param a_activity Activity info.
 */
void casper::job::Sequencer::TrackActivity (const casper::job::sequencer::Activity& a_activity)
{
    CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);
    
    // ... log ...
    SEQUENCER_LOG_ACTIVITY(CC_JOB_LOG_LEVEL_INF, a_activity, CC_JOB_LOG_STEP_STEP,
                           "%s", "Track"
    );
        
    // ... keep track of running activity ...
    running_activities_[a_activity.rcid()] = new casper::job::sequencer::Activity(a_activity);
    
    // ... schedule a timeout event for this activity ...
    ScheduleCallbackOnLooperThread(/* a_id */ a_activity.rcid(),
                                   /* a_callback */ std::bind(&casper::job::Sequencer::OnActivityTimeout, this, std::placeholders::_1),
                                   /* a_deferred  */ ( a_activity.ttr() * 1000 ) + 100 , // ttr + 100 milliseconds of threshold
                                   /* a_recurrent */ false
   );
    
    // ... log ...
    LogStats();
}

/**
 * @brief Track an activity.
 *
 * @param a_activity Activity info.
 */
void casper::job::Sequencer::TrackActivity (casper::job::sequencer::Activity* a_activity)
{
    CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);

    // ... log ...
    SEQUENCER_LOG_ACTIVITY(CC_JOB_LOG_LEVEL_INF, (*a_activity), CC_JOB_LOG_STEP_STEP,
                           "%s", "Track"
    );

    // ... keep track of running activity ...
    running_activities_[a_activity->rcid()] = a_activity;
    
    // ... schedule a timeout event for this activity ...
    ScheduleCallbackOnLooperThread(/* a_id */ a_activity->rcid(),
                                   /* a_callback */ std::bind(&casper::job::Sequencer::OnActivityTimeout, this, std::placeholders::_1),
                                   /* a_deferred  */ ( a_activity->ttr() * 1000 ) + 100 , // ttr + 100 milliseconds of threshold
                                   /* a_recurrent */ false
    );
    
    // ... log ...
    LogStats();
}

/**
 * @brief Untrack an activity that is or will not be running.
 *
 * @param a_activity Activity info.
*/
void casper::job::Sequencer::UntrackActivity (const casper::job::sequencer::Activity& a_activity)
{
    CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);
    
    // ... log ...
    SEQUENCER_LOG_ACTIVITY(CC_JOB_LOG_LEVEL_INF, a_activity, CC_JOB_LOG_STEP_STEP,
                           "%s", "Untrack"
    );

    // ... cancel previously schedule ( if any ) timeout event for this activity ...
    TryCancelCallbackOnLooperThread(a_activity.rcid());

    // ... ensure it's running ...
    const auto it = running_activities_.find(a_activity.rcid());
    if ( running_activities_.end() != it ) {
        delete it->second;
        running_activities_.erase(it);
    }
    
    // ... log ...
    LogStats();
}

/**
 * @brief Callback to execute upon an activity execution timedout.
 *
 * @param a_rcid Activity REDIS channel id.
 */
void casper::job::Sequencer::OnActivityTimeout (const std::string& a_rcid)
{
    CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);
    
    // ... activity still 'running'?
    const auto it = running_activities_.find(a_rcid);
    if ( running_activities_.end() == it ) {
        // ... not running, we're done ...
        return;
    }
    // ... mark as timed-out ...
    it->second->SetStatus(sequencer::Status::Failed);

    // ... log ...
    SEQUENCER_LOG_ACTIVITY(CC_JOB_LOG_LEVEL_INF, (*it->second), CC_JOB_LOG_STEP_STEP,
                           "Timed-out after " UINT32_FMT " second(s)", it->second->ttr()
    );
        
    Json::Value response;
    (void)SetTimeoutResponse(/* a_payload */ Json::Value::null, response);

    // ... signal activity 'failed' ...
    ActivityReturned(SEQUENCER_TRACK_CALL(it->second->sequence().bjid(), "ACTIVITY TIMEOUT"),
                     *it->second, &response
    );
    
    // ... debug only: ensure activity was untracked ...
    CC_IF_DEBUG({
        CC_ASSERT(running_activities_.end() == running_activities_.find(a_rcid));
    });
    
    // ... log ...
    LogStats();
}

#ifdef __APPLE__
#pragma mark -
#endif

/**
 * @brief Call when the 'final' activity was performed so we close the job.
 *
 * @param a_sequence Sequence info.
 * @param a_response Job response.
 */
void casper::job::Sequencer::FinalizeJob (const sequencer::Sequence& a_sequence,
                                          const Json::Value& a_response)
{
    CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);
    
    //
    // ... Notify 'deferred' JOB finalization ...
    //
    CC_WARNING_TODO("CJS: global broadcast job status");
    
    // ... publish result ...
    Finished(/* a_id               */ a_sequence.bjid(),
             /* a_channel          */ a_sequence.rcid(),
             /* a_response         */ a_response,
             /* a_success_callback */
             [this, &a_response]() {
                // ... log status ...
                EV_LOOP_BEANSTALK_JOB_LOG_QUEUE("STATUS", "%s",
                                                a_response["status"].asCString()
                );
             },
             /* a_failure_callback */
             [this](const ev::Exception& a_ev_exception) {
                // ... log status ...
                EV_LOOP_BEANSTALK_JOB_LOG_QUEUE("STATUS", "%s: %s",
                                                "EXCEPTION", a_ev_exception.what()
                );
            }
    );
}

/**
 * @brief Called by REDIS subscriptions manager.
 *
 * @param a_id      REDIS channel id.
 * @param a_status  Status flag value.
 * @param a_message Channel's message.
 */
void casper::job::Sequencer::OnJobsSignalReceived (const uint64_t& a_id, const std::string& a_status, const Json::Value& a_message)
{
    CC_DEBUG_FAIL_IF_NOT_AT_MAIN_THREAD();
    
    //
    // ⚠️ We're only using this callback to listen to 'cancellation' signals for sequencer's job
    //    NOT for sequences or activities - ( they have their own callbacks ).
    //
    if ( 0 != strcasecmp(a_status.c_str(), "cancelled") ) {
        // ... not interested ...
        return;
    }
    
    ScheduleCallbackOnLooperThread("sequence-jobs-signals-callback",
       [this, a_id, a_status, a_message](const std::string& /* a_id */) {
    
            CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);

        //
            // Process cancellation message.
            //
            const casper::job::sequencer::Tracking tracking = SEQUENCER_TRACK_CALL(static_cast<int64_t>(a_id), "JOBS SIGNALS MESSAGE RECEIVED");
        
            try {
                
                Json::Value response;
                
                // ... prepare response ...
                SetCancelledResponse(/* a_payload*/ a_message, /* o_response */ response);

                //
                // ⚠️ Since we're only running one activity at a time for a specific sequence,
                //    we can stop the search at the first activity that belongs to the sequence.
                //
                for ( auto it : running_activities_ ) {
                    // ... found?
                    if ( it.second->sequence().rjnr() == a_id) {
                        // .. copy sequence info ...
                        const sequencer::Sequence sequence = it.second->sequence();
                        // ... cancel ...
                        CancelSequence(*it.second, response);
                        // ... finish job ...
                        FinalizeJob(sequence, response);
                        // ... and we're done ...
                        break;
                    }
                }
                                
            } catch (const ::cc::Exception& a_cc_exception) {
                // ... log error ...
                SEQUENCER_LOG_JOB(CC_JOB_LOG_LEVEL_ERR, tracking.bjid_, CC_JOB_LOG_STEP_ERROR, "%s", a_cc_exception.what());
            }

        }
    );
}

#ifdef __APPLE__
#pragma mark -
#endif

/**
 * @brief Execute a PostgreSQL a batch of queries and wait for it's response.
 *
 * @param a_tracking Call tracking proposes.
 * @param a_query    SQL query to execute.
 * @param a_expected Expected returned status code, one of \link ExecStatusType \link.
 * @param a_callback Callback to deliver result.
 */
void casper::job::Sequencer::ExecuteQueryAndWait (const casper::job::sequencer::Tracking& a_tracking,
                                                  const std::string& a_query, const ExecStatusType& a_expected,
                                                  const std::function<void(const Json::Value& a_value)> a_success_callback,
                                                  const std::function<void(const ev::Exception& a_exception)> a_failure_callback)
{
    CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);
    
    // ... keep track if initial errors count ...
    
    const size_t ec = ErrorsCount();
    
    osal::ConditionVariable cv;
    cc::Exception*          ex    = nullptr;
    Json::Value*            table = nullptr;
    
    ExecuteOnMainThread([this, &a_query, &a_expected, &cv, &ex, &table] () {
        
        NewTask([this, &a_query] () -> ::ev::Object* {
            
            // ... execute query ...
            return new ev::postgresql::Request(loggable_data_, a_query);
                    
        })->Finally([this, &a_expected, &cv, &table] (::ev::Object* a_object) {

            // ... ensure query succeeded  ...
            const auto value = EnsurePostgreSQLValue(a_object, a_expected);
            // ... serialize to json ...
            if ( a_expected == ExecStatusType::PGRES_TUPLES_OK  ) {
                table = new Json::Value(Json::ValueType::arrayValue);
                ToJSON(*value, *table);
            }
            
            // ... RELEASE control ...
            cv.Wake();
            
        })->Catch([&cv, &ex] (const ::ev::Exception& a_ev_exception) {

            //
            ex = new ::cc::Exception(a_ev_exception);
            
            // ... RELEASE control ...
            cv.Wake();
            
        });
        
    }, /* a_blocking */ false);
    
    // ... WAIT ...
    cv.Wait();
        
    // ... if an exception is set ...
    if ( nullptr != ex ) {
        // ... notify ...
        if ( nullptr != a_failure_callback ) {
            TrapUnhandledExceptions(/* a_type */
                                    "PostgreSQL Query Execution Failed Notification Callback",
                                    /* a_callback */
                                    [&] () {
                                        a_failure_callback(*ex);
                                    },
                                    a_tracking
            );
        } else {
            // ... append error ...
            AppendError(/* a_type  */ "PostgreSQL Query Execution Failed Notification Callback",
                        /* a_why   */ ex->what(),
                        /* a_where */ __PRETTY_FUNCTION__,
                        /* a_code */ ev::loop::beanstalkd::Job::k_exception_rc_
            );
        }
    } else {
        // ... notify ...
        if ( nullptr != a_success_callback ) {
            TrapUnhandledExceptions(/* a_type */
                                    "PostgreSQL Query Execution Succeeded Notification Callback",
                                    /* a_callback */
                                    [&] () {
                                        a_success_callback(( nullptr != table ? *table : Json::Value::null ));
                                    },
                                    a_tracking
            );
        }
    }
    
    // ... cleanup ..
    if ( nullptr != ex ) {
        delete ex;
    }
    if ( nullptr != table ) {
        delete table;
    }
    
    // ... if errors count changed
    if ( ErrorsCount() != ec ) {
        throw sequencer::JumpErrorAlreadySet(a_tracking, /* a_code */ 500, LastError()["why"].asCString());
    }
}

/**
 * @brief Ensure a valid PostgreSQL value.
 *
 * @param a_object   Object to be tested.
 * @param a_expected Expected returned status code, one of \link ExecStatusType \link.
 *
 * @return PostgreSQL value object.
 */
const ::ev::postgresql::Value* casper::job::Sequencer::EnsurePostgreSQLValue (const ::ev::Object* a_object, const ExecStatusType& a_expected)
{
    const ::ev::Result* result = dynamic_cast<const ::ev::Result*>(a_object);
    if ( nullptr == result ) {
        throw ::ev::Exception("Unexpected PostgreSQL result object: nullptr!");
    }
    
    const ::ev::postgresql::Reply* reply = dynamic_cast<const ::ev::postgresql::Reply*>(result->DataObject());
    if ( nullptr == reply ) {
        const ::ev::postgresql::Error* error = dynamic_cast<const ::ev::postgresql::Error*>(result->DataObject());
        if ( nullptr != error ) {
            throw ::ev::Exception("%s", error->message().c_str());
        } else {
            throw ::ev::Exception("Unexpected PostgreSQL data object!");
        }
    } else {
        if ( reply->value().is_error() ) {
            throw ::ev::Exception("%s", reply->value().error_message());
        } else {
            if ( a_expected != reply->value().status() ) {
                throw ::ev::Exception("Unexpected PostgreSQL status: got " UINT8_FMT " expecting " UINT8_FMT, (uint8_t)reply->value().status(), (uint8_t)a_expected);
            }
        }
    }
    
    return &reply->value();
}

void casper::job::Sequencer::TrapUnhandledExceptions (const char* const a_type, const std::function<void()>& a_callback,
                                                      const casper::job::sequencer::Tracking& a_tracking)
{
    CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);
    
    try {
        // ... perform callback ...
        a_callback();
    } catch (const cc::Exception& a_cc_exception) {
        // ... track error ...
        AppendError(/* a_type  */ std::string(a_tracking.action_ + ':' + a_type).c_str(),
                    /* a_why   */ a_cc_exception.what(),
                    /* a_where */ a_tracking.function_.c_str(),
                    /* a_code */ casper::job::Sequencer::k_exception_rc_
        );
    } catch (...) {
        try {
            // ... rethrow ...
            ::cc::Exception::Rethrow(/* a_unhandled */ true, a_tracking.file_.c_str(), a_tracking.line_, a_tracking.function_.c_str());
        } catch (::cc::Exception& a_cc_exception) {
            // ... track error ...
            AppendError(/* a_type  */ std::string(a_tracking.action_ + ':' + a_type).c_str(),
                        /* a_why   */ a_cc_exception.what(),
                        /* a_where */ a_tracking.function_.c_str(),
                        /* a_code */ ev::loop::beanstalkd::Job::k_exception_rc_
            );
        }
    }
}

/**
 * @brief Serialize a JSON string to a JSON Object.
 *
 * @param a_value JSON string to parse.
 * @param o_value JSON object to fill.
*/
const Json::Value& casper::job::Sequencer::AsJSON (const std::string& a_value, Json::Value& o_value)
{
    CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);
    
    ParseJSON(a_value, o_value);
    
    return o_value;
}

/**
 * @brief Patch an activitiy payload using V8.
 *
 * @param a_tracking Call tracking proposes.
 * @param a_activity Activity info.
 */
void casper::job::Sequencer::PatchActivity (const casper::job::sequencer::Tracking& a_tracking,
                                            casper::job::sequencer::Activity& a_activity)
{
    CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);
        
    // ... load previous activities responses to V8 engine ..
    std::stringstream ss; ss.clear(); ss.str("");
    
    // ... js.get_activities_responses (sid INTEGER) ...
    ss << "SELECT * FROM js.get_activities_responses(";
    ss <<  a_activity.sequence().did();
    ss << ");";
    
    
    Json::Value object = Json::Value::null;
    
    // ... register @ DB ...
    ExecuteQueryAndWait(/* a_tracking         */ SEQUENCER_TRACK_CALL(a_activity.sequence().bjid(), "GETTING ACTIVITIES RESPONSES"),
                        /* a_query            */ ss.str(), /* a_expected */ ExecStatusType::PGRES_TUPLES_OK,
                        /* a_success_callback */
                        [&object] (const Json::Value& a_value) {
        
                            //
                            // EXPECTING:
                            //
                            // {
                            //    "id" : <numeric>,
                            //    "index" : <numeric>,
                            //    "response" : <json_array>,
                            //    "sid" : <numeric>,
                            //    "status" : <string>
                            // }
                            //
        
                            if ( 0 == a_value.size() ) {
                                return;
                            }
        
                            const Json::Value& first = a_value[0];
                            if ( true == first["id"].isNull() || false == first.isMember("sequence") ) {
                                return;
                            }
        
                            object              = first["sequence"];
                            object["responses"] = Json::Value(Json::ValueType::arrayValue);
                            object["responses"].append(first["response"]);
                
                            for ( Json::ArrayIndex idx = 1 ; idx < a_value.size() ; ++idx ) {
                                if ( false == a_value[idx]["id"].isNull() && true == a_value[idx].isMember("sequence") ) {
                                    object["responses"].append(a_value[idx]["response"]);
                                }
                            }

                        }
    );
    
    // ... data must be previously set on DB ...
    if ( true == object.isNull() ) {
        throw sequencer::Exception(a_tracking, /* a_code */ 500, "No data available for this activity ( from db )!");
    }
    
    //
    // V8 evaluation
    //
    ::v8::Persistent<::v8::Value> data;

    // ... load data to V8 ...
    try {
        
        // ... log ...
        SEQUENCER_LOG_ACTIVITY(CC_JOB_LOG_LEVEL_VBS, a_activity, CC_JOB_LOG_STEP_V8,
                               "%s", "Loading data object");
        if ( 0 == a_activity.index() ) {
            // ... log ...
            SEQUENCER_LOG_SEQUENCE(CC_JOB_LOG_LEVEL_INF, a_activity.sequence(), CC_JOB_LOG_STEP_V8,
                                   "Data object: " CC_JOB_LOG_COLOR(LIGHT_BLUE) "%s" CC_LOGS_LOGGER_RESET_ATTRS,
                                   jfw_.write(object).c_str()
            );
        } else {
            // ... log ...
            SEQUENCER_LOG_SEQUENCE(CC_JOB_LOG_LEVEL_VBS, a_activity.sequence(), CC_JOB_LOG_STEP_V8,
                                   "%s", "Data object ~ <dump skipped>");
        }

        // ... set v8 value ...
        Json::FastWriter fw; fw.omitEndingLineFeed();
        script_->SetData(/* a_name  */ ( a_activity.rjid() + "-v8-data" ).c_str(),
                         /* a_data   */ fw.write(object).c_str(),
                         /* o_object */ nullptr,
                         /* o_value  */ &data,
                         /* a_key    */ nullptr
        );

        // ... log ...
        SEQUENCER_LOG_ACTIVITY(CC_JOB_LOG_LEVEL_INF, a_activity, CC_JOB_LOG_STEP_V8,
                               "%s", "Data object loaded");

    } catch (const ::cc::v8::Exception& a_v8e) {
        throw sequencer::V8ExpressionEvaluationException(a_tracking, a_v8e);
    }
        
    Json::Value payload = a_activity.payload();
    
    // ... log ...
    SEQUENCER_LOG_ACTIVITY(CC_JOB_LOG_LEVEL_VBS, a_activity, CC_JOB_LOG_STEP_V8,
                           "Patching payload: " CC_JOB_LOG_COLOR(WHITE) "%s" CC_LOGS_LOGGER_RESET_ATTRS,
                           jfw_.write(payload).c_str()
    );
    
    // ... traverse JSON and evaluate 'String' fields ...
    ::cc::v8::Value value;
    PatchObject(payload, [this, &a_tracking, &data, &value] (const std::string& a_expression) -> Json::Value {
        // ... start as null ...
        value.SetNull();
        // ...
        try {
            script_->Evaluate(data, a_expression, value);
        } catch (const ::cc::v8::Exception& a_v8e) {
            throw sequencer::V8ExpressionEvaluationException(a_tracking, a_v8e);
        }
        // ...
        switch(value.type()) {
            case ::cc::v8::Value::Type::Int32:
                return Json::Value(value.operator int());
            case ::cc::v8::Value::Type::UInt32:
                return Json::Value(value.operator unsigned int());
            case ::cc::v8::Value::Type::Double:
                return Json::Value(value.operator double());
            case ::cc::v8::Value::Type::String:
                return Json::Value(value.AsString());
            case ::cc::v8::Value::Type::Boolean:
                return Json::Value(value.operator const bool());
            case ::cc::v8::Value::Type::Object:
                return value.operator const Json::Value &();
            case ::cc::v8::Value::Type::Undefined:
            case ::cc::v8::Value::Type::Null:
                return Json::Value(Json::Value::null);
        }
    });
    
    // ... log ...
    SEQUENCER_LOG_ACTIVITY(CC_JOB_LOG_LEVEL_INF, a_activity, CC_JOB_LOG_STEP_V8,
                           "Payload patched: " CC_JOB_LOG_COLOR(LIGHT_CYAN) "%s" CC_LOGS_LOGGER_RESET_ATTRS,
                           jfw_.write(payload).c_str()
    );

    // ... set patched payload as activity new payload ....
    a_activity.SetPayload(payload);
}

/**
 * @brief
 *
 * @param a_value
 * @param a_callback
 */
void casper::job::Sequencer::PatchObject (Json::Value& a_object, const std::function<Json::Value(const std::string& a_expression)>& a_callback)
{
    CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);
    
    switch ( a_object.type() ) {
        case Json::ValueType::objectValue:   // object value (collection of name/value pairs)
            for( auto member : a_object.getMemberNames()) {
                PatchObject(a_object[member], a_callback);
            }
            break;
        case Json::ValueType::arrayValue:    // array value (ordered list)
            for ( auto ait = a_object.begin(); ait != a_object.end(); ++ait ) {
                PatchObject(*ait, a_callback);
            }
            break;
        case Json::ValueType::stringValue:   // UTF-8 string value
            if ( nullptr != strstr(a_object.asCString(), "$.") ) {
                a_object = a_callback(a_object.asString());
            }
            break;
        case Json::ValueType::nullValue:    // 'null' value
        case Json::ValueType::intValue:     // signed integer value
        case Json::ValueType::uintValue:    // unsigned integer value
        case Json::ValueType::realValue:    // double value
        case Json::ValueType::booleanValue: // bool value
        default:
            break;
    }
}

