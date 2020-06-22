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

#include "cc/debug/types.h"

CC_WARNING_TODO("CJS: review all comments and parameters names");
CC_WARNING_TODO("CJS: WRITE SOME OF THE DEBUG MESSAGES TO PERMANENT LOG!!");

#define CC_TRACK_CALL(a_bjid, a_action) \
    { a_bjid, a_action, __FILE__, __PRETTY_FUNCTION__, __LINE__ }

#ifdef __APPLE__
#pragma mark - casper::job::Sequencer
#endif

const char* const casper::job::Sequencer::s_tube_   = "casper-job-sequencer";
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
 * param a_loggable_data
 * param a_config
 */
casper::job::Sequencer::Sequencer (const ev::Loggable::Data& a_loggable_data, const cc::job::easy::Job::Config& a_config)
    : cc::job::easy::Job(a_loggable_data, casper::job::Sequencer::s_tube_, a_config),
      thread_id_(cc::debug::Threading::GetInstance().CurrentThreadID())
{
    json_writer_.omitEndingLineFeed();
}

/**
 * @brief Destructor
 */
casper::job::Sequencer::~Sequencer ()
{
    // ... cancel any subscriptions ...
    ExecuteOnMainThread([this] {
        ::ev::redis::subscriptions::Manager::GetInstance().Unubscribe(this);
    }, /* a_blocking */ true);
}

#ifdef __APPLE__
#pragma mark -
#endif

/**
 * @brief One-shot initialization.
 */
void casper::job::Sequencer::Setup ()
{
    /* empty */
}

#ifdef __APPLE__
#pragma mark -
#endif

/**
 * @brief Process a job to this tube.
 *
 * @param a_id      Job ID.
 * @param a_payload Job payload.
 *
 * @param o_response JSON object.
 */
void casper::job::Sequencer::Run (const int64_t& a_id, const Json::Value& a_payload,
                                  cc::job::easy::Job::Response& o_response)
{
    CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);
    /* TODO implement */
    //   {
    //      "id": "1",
    //      "ttr": 360,
    //      "valitity": 500,
    //      "tube": "casper-job-sequencer",
    //      "jobs": [
    //          {
    //              "relay": false,
    //              "payload": {
    //                  "tube": "tube-one",
    //                  "ttr": 120,
    //                  "validity": 70
    //              }
    //          },
    //          {
    //              "relay": true,
    //              "payload": {
    //                  "tube": "tube-two",
    //                  "ttr": 120,
    //                  "validity": 70
    //              }
    //          }
    //      ]
    //  }

    // ... start as a 'bad request' ...
    o_response.code_ = 400;
    
    // ... validate job payload ...
    CC_WARNING_TODO("CJS: validate job payload");
    
    try {
        
        //
        // NGINX-BROKER 'jobify' module awareness
        //
        const Json::Value* payload;
        if ( true == a_payload.isMember("body") && true == a_payload.isMember("headers") ) {
            // ... from nginx-broker 'jobify' module ...
            payload = &a_payload["body"];
        } else {
            // ... direct from beanstalkd queue ...
            payload = &a_payload;
        }

        const casper::job::Sequencer::Tracking tracking = CC_TRACK_CALL(a_id, "RUN JOB");
        
        // ... register sequence and grab first activity ...
        try {

            uint64_t rjnr = 0;
            if ( 1 != sscanf(a_payload["id"].asCString(), "" UINT64_FMT, &rjnr ) ) {
                throw Sequencer::JSONValidationException(tracking, ( "Unable to parse job id from '" + a_payload["id"].asString() + "'!" ).c_str());
            }

            // ... create sequence from payload ...
            auto sequence = casper::job::sequencer::Sequence(
                                                             /* a_source */ ( true == a_payload.isMember("body") && true == a_payload.isMember("headers")
                                                                                ? sequencer::Sequence::Source::Jobification
                                                                                : sequencer::Sequence::Source::Default
                                                             ),
                                                             /* a_cid */  config_.instance_,
                                                             /* a_bjid */ tracking.bjid_,
                                                             /* a_rjnr */ rjnr,
                                                             /* a_rjid */ ( config_.service_id_ + ":jobs:" + tube_ + ':' + a_payload["id"].asString() ),
                                                             /* a_rcid */ ( config_.service_id_ + ':'      + tube_ + ':' + a_payload["id"].asString() )
            );
            // ... register sequence ...
            auto first_activity = RegisterSequence(sequence, *payload);
            try {
                // ... launch first activity ...
                o_response.code_ = LaunchActivity(first_activity);
            } catch (const casper::job::Sequencer::JSONValidationException& a_jve) {
                // ... fallthrough to outer try - catch ...
                throw a_jve;
            } catch (...) {
                 // ... untrack activity ...
                 UntrackActivity(first_activity);
                 // ... rethrow ...
                 try {
                     ::cc::Exception::Rethrow(/* a_unhandled */ false,  tracking.file_.c_str(), tracking.line_, tracking.function_.c_str());
                } catch (::cc::Exception& a_cc_exception) {
                    // ... track error ...
                    AppendError(/* a_type  */ tracking.action_.c_str(),
                                /* a_why   */ a_cc_exception.what(),
                                /* a_where */ tracking.function_.c_str(),
                                /* a_code */  ev::loop::beanstalkd::Job::k_exception_rc_
                    );
                }
                // ... jump for common exception handling ...
                throw casper::job::Sequencer::JumpErrorAlreadySet(tracking, /* o_code */ 500, LastError()["why"].asCString());
            }
        } catch (const casper::job::Sequencer::JSONValidationException& a_jve) {
            // ... track 'SEQUENCE' or 'ACTIVITY' payload error ...
            AppendError(/* a_type  */ a_jve.tracking_.action_.c_str(),
                        /* a_why   */ a_jve.what(),
                        /* a_where */ a_jve.tracking_.function_.c_str(),
                        /* a_code */  ev::loop::beanstalkd::Job::k_exception_rc_
            );
            // ... jump for common exception handling ...
            throw casper::job::Sequencer::JumpErrorAlreadySet(tracking, a_jve.code_ ,LastError()["why"].asCString());
        }
        
    } catch (const casper::job::Sequencer::JumpErrorAlreadySet& a_jeas) {
        // ... set response code ...
        o_response.code_ = a_jeas.code_;
        // ... debug ...
        CC_DEBUG_LOG_TRACE("job", "Job #" INT64_FMT " ~= ERROR JUMP =~\n\nORIGIN: %s:%d\nACTION: %s\n\%s\n",
                           a_jeas.tracking_.bjid_,
                           a_jeas.tracking_.function_.c_str(), a_jeas.tracking_.line_,
                           a_jeas.tracking_.action_.c_str(),
                           a_jeas.what()
        );
    }
    
    // ... if scheduled, then the response must be deferred ...
    if ( 200 == o_response.code_ ) {
        // ... this will remove job from beanstalkd queue, but keeps the redis status as is ( in-progress ) ...
        SetDeferred();
    }
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
casper::job::sequencer::Activity casper::job::Sequencer::RegisterSequence (casper::job::sequencer::Sequence& a_sequence, const Json::Value& a_payload)
{
    CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);
    
    // ... debug ...
    CC_DEBUG_LOG_TRACE("job", "Job #" INT64_FMT " ~= registering sequencer's job",
                       a_sequence.bjid()
    );
    
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
    CC_WARNING_TODO("CJS: read defaults from config, ttr and valitity");
    CC_WARNING_TODO("CJS: validate payload");
    
    std::stringstream ss; ss.clear(); ss.str("");
    Json::FastWriter  jw; jw.omitEndingLineFeed();
    
    // ... js.register_sequence (pid INTEGER, cid INTEGER, bjid INTEGER, rjid TEXT, rcid TEXT, payload JSONB, activities JSONB, ttr INTEGER, validity INTEGER) ...
    ss << "SELECT * FROM js.register_sequence(";
    ss <<     config_.pid_ << ',' << a_sequence.cid() << ',' << a_sequence.bjid();
    ss <<     ',' <<  "'" << a_sequence.rjid() << "'" << ',' << "'" << a_sequence.rcid() << "'";
    ss <<     ",'" << ::ev::postgresql::Request::SQLEscape(jw.write(a_payload)) << "'";
    ss <<     ",'" << ::ev::postgresql::Request::SQLEscape(jw.write(a_payload["jobs"])) << "'";
    ss <<     ',' << a_payload["ttr"].asUInt64() << ',' << a_payload["validity"].asUInt64();
    ss << ");";
    
    //
    Json::Value activity = Json::Value::null;
    
    // ... register @ DB ...
    ExecuteQueryAndWait(/* a_tracking         */ CC_TRACK_CALL(a_sequence.bjid(), "REGISTERING SEQUENCE"),
                        /* a_query            */ ss.str(), /* a_expected */ ExecStatusType::PGRES_TUPLES_OK,
                        /* a_success_callback */
                        [&a_sequence, &activity] (const Json::Value& a_value) {
                            activity = a_value[0];
                        }
    );
    
    // ... debug ...
    CC_DEBUG_LOG_TRACE("job", "Job #" INT64_FMT " ~= sequencer job registered - did = %s",
                       a_sequence.bjid(), a_sequence.did().c_str()
    );

    // ... debug ...
    CC_DEBUG_LOG_TRACE("job", "Job #" INT64_FMT " ~= first activity:\n\n%s",
                       a_sequence.bjid(), activity.toStyledString().c_str()
    );

    CC_WARNING_TODO("CJS: inner jobs defaults by config");

    const Json::Value s_activity_default_validity_ = static_cast<Json::UInt64>(3600);
    const Json::Value s_activity_default_ttr_      = static_cast<Json::UInt64>(300);

    const auto did      = GetJSONObject(activity, "id"     , Json::ValueType::intValue   , /* a_default */ nullptr).asString();
    const auto job      = GetJSONObject(activity, "job"     , Json::ValueType::objectValue, /* a_default */ nullptr);
    const auto tube     = GetJSONObject(job     , "tube"    , Json::ValueType::stringValue, /* a_default */ nullptr).asString();
    const auto validity = GetJSONObject(job     , "validity", Json::ValueType::intValue   , &s_activity_default_validity_).asUInt();
    const auto ttr      = GetJSONObject(job     , "ttr"     , Json::ValueType::intValue   , &s_activity_default_ttr_).asUInt();
    const auto sid      = GetJSONObject(activity, "sid"     , Json::ValueType::intValue   , /* a_default */ nullptr).asString();
    
    // ... register sequence id from DB ...
    a_sequence.SetDID(did);
    
    // ... return first activity properties ...
    return sequencer::Activity(a_sequence, /* a_id */ sid, /* a_index */ 0, /* a_attempt */ 0).Bind(sequencer::Status::Pending, validity, ttr, activity);
}

/**
 * @brief Call when the 'final' activity was performed so we close the sequence.
 *
 * @param a_activity Activity info.
 */
void casper::job::Sequencer::FinalizeSequence (const casper::job::sequencer::Activity& a_activity)
{
    CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);
    
    const auto& sequence = a_activity.sequence();
    
    // ... debug ...
    CC_DEBUG_LOG_TRACE("job", "Job #" INT64_FMT " ~= finalizing job sequence - did = %s",
                       sequence.bjid(), sequence.did().c_str()
    );
    
    std::stringstream ss; ss.clear(); ss.str("");
    Json::FastWriter  jw; jw.omitEndingLineFeed();
    
    // ... js.finalize_sequence (id INTEGER, status js.status, response JSONB) ...
    ss << "SELECT * FROM js.finalize_sequence(";
    ss <<   sequence.did() << ',' << a_activity.status();
    ss <<   ',' << ",'" << ::ev::postgresql::Request::SQLEscape(jw.write(a_activity.payload())) << "'";
    ss << ");";
    
    // ... register @ DB ...
    ExecuteQueryAndWait(/* a_tracking */ CC_TRACK_CALL(sequence.bjid(), "FINALIZING JOB SEQUENCE"),
                        /* a_query    */ ss.str(), /* a_expected */ ExecStatusType::PGRES_TUPLES_OK
    );
    
    // ... debug ...
    CC_DEBUG_LOG_TRACE("job", "Job #" INT64_FMT " ~= finalized job sequence - did = %s",
                       sequence.bjid(), sequence.did().c_str()
    );
}

#ifdef __APPLE__
#pragma mark -
#endif

/**
 * @brief Launch an activity ( a.k.a inner job ).
 *
 * @param a_activity Activity info.
 * @param a_activity Activity payload ( if set it will be used, otherwise a_activity.payload_ must be set! ).
 *
 * @return HTTP status code.
 */
uint16_t casper::job::Sequencer::LaunchActivity (casper::job::sequencer::Activity& a_activity)
{
    CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);
    
    const casper::job::Sequencer::Tracking tracking = CC_TRACK_CALL(a_activity.sequence().bjid(), "LAUNCH ACTIVITY");
    
    // ... debug ...
    CC_DEBUG_LOG_TRACE("job", "Job #" INT64_FMT " ~= launching activity #" SIZET_FMT,
                       a_activity.sequence().bjid(), a_activity.index()
    );
        
    const std::string id_key = config_.service_id_ + ":jobs:sequential_id";

    CC_WARNING_TODO("CJS: VALIDATE a_payload - ensure tube is set");

    typedef struct {
        std::string tube_;
        int64_t     expires_in_;
        uint32_t    ttr_;
        std::string id_;
        std::string key_;
        std::string channel_;
        uint16_t    sc_;
    } ActivityJob;
    
    ActivityJob job_defs;
    
    // ... debug ...
    CC_DEBUG_LOG_TRACE("job", "Job #" INT64_FMT " ~= activity #" SIZET_FMT " payload:\n\n%s" ,
                       a_activity.sequence().bjid(), a_activity.index(), a_activity.payload().toStyledString().c_str()
    );
    
    try {
        
        CC_WARNING_TODO("CJS: inner jobs defaults by config");
        
        const Json::Value s_activity_default_validity_ = static_cast<Json::UInt64>(3600);
        const Json::Value s_activity_default_ttr_      = static_cast<Json::UInt64>(300);
        
        const auto job       = GetJSONObject(a_activity.payload(), "job"     , Json::ValueType::objectValue, /* a_default */ nullptr);
        job_defs.tube_       = GetJSONObject(job                 , "tube"    , Json::ValueType::stringValue, /* a_default */ nullptr).asString();
        job_defs.expires_in_ = GetJSONObject(job                 , "validity", Json::ValueType::intValue   , &s_activity_default_validity_).asUInt();
        job_defs.ttr_        = GetJSONObject(job                 , "ttr"     , Json::ValueType::intValue   , &s_activity_default_ttr_).asUInt();
        
    } catch (const ev::Exception& a_ev_exception) {
        throw Sequencer::JSONValidationException(tracking, a_ev_exception.what());
    }
    
    job_defs.id_         = "";
    job_defs.key_        = ( config_.service_id_ + ":jobs:" + job_defs.tube_ + ':' );
    job_defs.channel_    = ( config_.service_id_ + ':' + job_defs.tube_ + ':'      );
    job_defs.sc_         = 500;

    osal::ConditionVariable cv;
        
    ExecuteOnMainThread([this, &a_activity, &cv, &job_defs, &id_key, &tracking] () {

        NewTask([this, &job_defs, &id_key] () -> ::ev::Object* {
            
            // ...  get new job id ...
            return new ::ev::redis::Request(loggable_data_, "INCR", {
                /* key   */ id_key
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
            
            job_defs.id_       = std::to_string(value.Integer());
            job_defs.key_     += job_defs.id_;
            job_defs.channel_ += job_defs.id_;
            
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
            
        })->Finally([this, &cv, &job_defs] (::ev::Object* a_object) {
            
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
            
        })->Catch([this, &cv, &job_defs, &tracking] (const ::ev::Exception& a_ev_exception) {
            
            job_defs.sc_ = 500;
            
            CC_WARNING_TODO("CJS: BETTER EXCEPTION REPORT - SEE, merge and fix ExecuteQueryAndWait");
            
            const std::string prefix = "rror while setting up REDIS job to tube " + job_defs.tube_;
            
            AppendError(/* a_type  */ "REDIS",
                        /* a_why   */ ( 'E'+ prefix + ": " + a_ev_exception.what() ).c_str(),
                        /* a_where */ tracking.function_.c_str(),
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
    
    //
    //  ... prepare, register and push activity job ...
    //
    try {
        // ... bind ids ...
        a_activity.Bind(/* a_rjid */ job_defs.key_, /* a_rcid */ job_defs.channel_);
        // ... grab job object ...
        const auto job = GetJSONObject(a_activity.payload(), "job" , Json::ValueType::objectValue, /* a_default */ nullptr);
        // .. first, copy payload ( so it can be patched ) ...
        Json::Value payload = job["payload"];
        // ... log ...
        CC_DEBUG_LOG_TRACE("job", "Job #" INT64_FMT " ~= patching job #" SIZET_FMT " - %s",
                           a_activity.sequence().bjid(), a_activity.index(), a_activity.rcid().c_str()
        );
        // ... debug only ...
        CC_DEBUG_LOG_MSG("job", "before patch:\n%s",  payload.toStyledString().c_str());
        // ... if required, evaluate all string fields as v8 expressions ...
        if ( a_activity.index() > 0 ) {
            // ... required ...
            CC_WARNING_TODO("CJS: EVALUATE V8 EXPRESSIONS");
        }
        // ... set or overwrite 'id' and 'tube' properties ...
        payload["id"]   = job_defs.id_;
        payload["tube"] = job_defs.tube_;
        // ... debug only ...
        CC_DEBUG_LOG_MSG("job", "after patch:\n%s",  payload.toStyledString().c_str());
        // ... log ...
        CC_DEBUG_LOG_TRACE("job", "Job #" INT64_FMT " ~= patched job #" SIZET_FMT " - %s",
                           a_activity.sequence().bjid(), a_activity.index(), a_activity.rcid().c_str()
        );
        // ... tmp track payload ...
        a_activity.SetPayload(payload);
        // ... now register activity attempt to launch @ db ...
        RegisterActivity(a_activity);
        // ... track activity ...
        TrackActivity(a_activity);
        // ... then, listen to REDIS job channel ...
        SubscribeActivity(a_activity);
        // ... now, push job ( send it to beanstalkd ) ...
        PushActivity(a_activity);
    } catch (...) {
        CC_WARNING_TODO("CJS: TEST THIS CODE");
        // ... forget tmp payload ...
        a_activity.SetPayload(Json::Value::null);
        // ... and this activity ...
        UntrackActivity(a_activity); // ... ⚠️  a_activity STILL valid - it's the original one! ⚠️ ...
        // ... recapture exception ...
        try {
            // ... first rethrow ...
            ::cc::Exception::Rethrow(/* a_unhandled */ true, __FILE__, __LINE__, __FUNCTION__);
        } catch (::cc::Exception& a_cc_exception) {
            // ... log ...
            CC_DEBUG_LOG_TRACE("job", "Job #" INT64_FMT " ~= an error occurred while launching activity #" SIZET_FMT "\n%s",
                               a_activity.sequence().bjid(), a_activity.index(), a_cc_exception.what()
            );
            // ... prepare response ...
            Json::Value response = Json::Value(Json::objectValue);
            response["cc-exception"] = a_cc_exception.what();
            // ... reset
            a_activity.Reset(sequencer::Status::Failed, /* a_payload */ response, /* a_ttr */ 0);
            // ... finalize activity ( by setting failed status ) ...
            (void)ActivityReturned(a_activity, /* a_response */ nullptr);
            // ... rethrow exception - this job FAILED ...
            throw a_cc_exception;
        }
    }
    
    // ... reset ptr ...
    a_activity.SetPayload(Json::Value::null);
    
    // ... debug ...
    CC_DEBUG_LOG_TRACE("job", "Job #" INT64_FMT " ~= activity #" SIZET_FMT " launched: %s",
                       a_activity.sequence().bjid(), a_activity.index(), a_activity.rcid().c_str()
    );
    
    // ... we're done ...
    return job_defs.sc_;
}

/**
 * @brief Call this method when an activity has a message to be relayed to the sequencer job.
 *
 * @param a_activity Activity info.
 * @param a_message  JSON object to relay.
 *
 * @return HTTP status code.
*/
void casper::job::Sequencer::ActivityMessageRelay (const casper::job::sequencer::Activity& a_activity, const Json::Value& a_message)
{
    CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);
    
    const uint64_t    bjid            = a_activity.sequence().bjid();
    const size_t      aidx            = a_activity.index();
    const uint64_t    dst_channel_nr  = a_activity.sequence().rjnr();
    const std::string src_channel_key = a_activity.rcid();
    const std::string dst_channel_key = a_activity.sequence().rcid();

    // ... debug ...
    CC_DEBUG_LOG_TRACE("job", "Job #" INT64_FMT " ~= activity #" SIZET_FMT " relay message from %s to %s\n\n%s\n",
                       bjid, aidx, src_channel_key.c_str(), dst_channel_key.c_str(), a_message.toStyledString().c_str()
    );
        
    try {
        Relay(dst_channel_nr, dst_channel_key, a_message);
    } catch (...) {
        try {
            ::cc::Exception::Rethrow(/* a_unhandled */ true, __FILE__, __LINE__, __FUNCTION__);
        } catch (const ::cc::Exception& a_cc_exception) {
            // ... debug ...
            CC_DEBUG_LOG_TRACE("job", "Job #" INT64_FMT " ~= activity #" SIZET_FMT " FAILED to relay message from %s to %s\n\n%s\n",
                               bjid, aidx, src_channel_key.c_str(), dst_channel_key.c_str(), a_cc_exception.what()
            );
        }
    }
}

/**
 * @brief Call this method when an activity has returned ( the next in sequence will be launched if needed ).
 *
 * @param a_activity Activity info.
 * @param a_response Activity response.
 *
 * @return HTTP status code.
 */
void casper::job::Sequencer::ActivityReturned (const casper::job::sequencer::Activity& a_activity, const Json::Value* a_response)
{
    CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);
    
    // ... debug ...
    CC_DEBUG_LOG_TRACE("job", "Job #" INT64_FMT " ~= activity #" SIZET_FMT " returned",
                       a_activity.sequence().bjid(), a_activity.index()
    );
    
    // ... first copy data ...
    sequencer::Activity copy = sequencer::Activity(a_activity);
    
    // ... prepare next activity ...
    sequencer::Activity next = sequencer::Activity(/* a_sequence */ a_activity.sequence(), /* a_id */ a_activity.did(), /* a_index */ copy.index() + 1, /* a_attempt */ 0);
    
    // ... finalize activity and pick next ( if any ) ...
    FinalizeActivity(copy, a_response, next);
    
    // ... copy activity ...
    const auto activity_copy = sequencer::Activity(a_activity);
    
    // ... untrack activity ...
    UntrackActivity(a_activity); // ... ⚠️ from now on a_activity is NOT valid ! ⚠️ ...
    
    // ... do we have another activity?
    if ( sequencer::Status::Pending == next.status() ) {
        // ... we're ready to next activity ...
        LaunchActivity(next);
    } else {

        const Json::Value* job_response;
        
        // ... we're done or a critical error occurred ...
        if ( sequencer::Status::Done == next.status() ) {
            // ... we're done ...
            job_response = a_response;
        } else {
            // ... critical error ...
            job_response = &next.payload();
        }
        
        //
        // ... ⚠️ WARNING: emulating same beahaviour as cc::job::easy::Job::Run ...
        //
        const std::string response_str = json_styled_writer_.write(*job_response);
        
        CC_DEBUG_LOG_TRACE("job", "Job #" INT64_FMT " ~> status: %s", activity_copy.sequence().bjid(), (*job_response)["status"].asCString());
                
        // ... publish result ...
        Finished(/* a_id               */  activity_copy.sequence().rjnr(),
                 /* a_channel          */ activity_copy.sequence().rcid(),
                 /* a_response         */ *job_response,
                 /* a_success_callback */
                 [this, &activity_copy, &response_str]() {
                    CC_DEBUG_LOG_TRACE("job", "Job #" INT64_FMT " ~> response:\n%s", activity_copy.sequence().bjid(), response_str.c_str());
                 },
                 /* a_failure_callback */
                 [this, &activity_copy](const ev::Exception& a_ev_exception){
                    CC_DEBUG_LOG_TRACE("job", "Job #" INT64_FMT " ~> exception: %s", activity_copy.sequence().bjid(), a_ev_exception.what());
                    (void)(a_ev_exception);
                }
        );
        
        // ... log status ...
        EV_LOOP_BEANSTALK_JOB_LOG_QUEUE("STATUS", "%s",
                                        (*job_response)["status"].asCString()
        );
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
    
    // ... debug ...
    CC_DEBUG_LOG_TRACE("job", "Job #" INT64_FMT " ~= registering activity #" SIZET_FMT,
                       a_activity.sequence().bjid(), a_activity.index()
    );

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
      
    // ... js.register_activity (sid INTEGER, id INTEGER, attempt INTEGER, payload JSONB, status js.status); ...
    ss << "SELECT * FROM js.register_activity(";
    ss <<   a_activity.sequence().did() << ',' << a_activity.did() << ',' << a_activity.attempt();
    ss <<   ",'" << ::ev::postgresql::Request::SQLEscape(jw.write(data)) << "'";
    ss <<   ",'" << sequencer::Status::InProgress << "'";
    ss << ");";
      
    // ... execute query ...
    ExecuteQueryAndWait(/* a_tracking */ CC_TRACK_CALL(a_activity.sequence().bjid(), "REGISTERING ACTIVITY"),
                        /* a_query    */ ss.str(), /* a_expected */ ExecStatusType::PGRES_TUPLES_OK
    );
    
    // ... debug ...
    CC_DEBUG_LOG_TRACE("job", "Job #" INT64_FMT " ~= activity #" SIZET_FMT " registered",
                       a_activity.sequence().bjid(), a_activity.index()
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
    
    // ... debug ...
    CC_DEBUG_LOG_TRACE("job", "Job #" INT64_FMT " ~= subscribing to activity #" SIZET_FMT " channel '%s'",
                       a_activity.sequence().bjid(), a_activity.index(), a_activity.rcid().c_str()
    );

    osal::ConditionVariable cv;
    
    ExecuteOnMainThread([this, &a_activity, &cv] () {
        
        ::ev::redis::subscriptions::Manager::GetInstance().SubscribeChannels({ a_activity.rcid()},
            [this, &a_activity, &cv](const std::string& a_id, const ::ev::redis::subscriptions::Manager::Status& a_status) -> EV_REDIS_SUBSCRIPTIONS_DATA_POST_NOTIFY_CALLBACK {
                // ... subscribed?
                if ( ::ev::redis::subscriptions::Manager::Status::Subscribed == a_status ) {
                    // ... debug ...
                    CC_DEBUG_LOG_TRACE("job", "Job #" INT64_FMT " ~= subscribed to activity #" SIZET_FMT " channel '%s'",
                                       a_activity.sequence().bjid(), a_activity.index(), a_activity.rcid().c_str()
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
    CC_DEBUG_FAIL_IF_NOT_AT_MAIN_THREAD();
    
    ExecuteOnLooperThread([this, a_id, a_message]() {

           CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);
        
           // ... log ...
           CC_DEBUG_LOG_TRACE("job", "Job #" INT64_FMT " ~= received message from channel '%s': %s",
                              static_cast<uint64_t>(0), a_id.c_str(), a_message.c_str()
           );
           
           // ... expecting message?
           const auto activity_it = running_activities_.find(a_id);
           if ( running_activities_.end() == activity_it ) {
               // ... log ...
               CC_DEBUG_LOG_TRACE("job", "Job #" INT64_FMT " ~= '%s': %s",
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
               CC_DEBUG_LOG_TRACE("job", "Job #" INT64_FMT " ~= '%s': status is %s",
                                  static_cast<uint64_t>(0), a_id.c_str(), status.c_str()
               );
                              
               // ... interest in this status ( completed, failed or cancelled ) ?
               const auto m_it = s_irj_teminal_status_map_.find(status);
               if ( s_irj_teminal_status_map_.end() == m_it ) {
                   //
                   // ... relay 'in-progress' messages ...
                   // ... ( because we may have more activities to run ) ...
                   if ( 0 == status.compare("in-progress") ) {
                       ActivityMessageRelay(*activity_it->second, object);
                   }
                   // ... no moe interest, we're done
                   return;
               }
                       
               //
               // ... we're interested:
               // ... ( completed, failed or cancelled )
               //
               // ... - we've got all required data to finalize this inner job
               // ... - we can launch the next inner job ( if required )
               //
               ActivityReturned(*activity_it->second, &object); // ... ⚠️ from now on a_activity is NOT valid ! ⚠️ ...
               
           } catch (const ::cc::Exception& a_cc_exception) {
               CC_WARNING_TODO("CJS: HANDLE EXCEPTIONS!!");
               CC_DEBUG_LOG_TRACE("job", "Job #" INT64_FMT "'%s': %s",
                                  static_cast<uint64_t>(0), a_id.c_str(), a_cc_exception.what()
               );
           }
        
           
    }, /* a_blocking */ false);
       
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
    
    // ... debug ...
    CC_DEBUG_LOG_TRACE("job", "Job #" INT64_FMT " ~= unsubscribing from activity #" SIZET_FMT " channel '%s'",
                       a_activity.sequence().bjid(), a_activity.index(), a_activity.rcid().c_str()
    );

    osal::ConditionVariable cv;
    
    ExecuteOnMainThread([this, &a_activity, &cv] () {
        
        ::ev::redis::subscriptions::Manager::GetInstance().UnsubscribeChannels({ a_activity.rcid() },
            [this,  &a_activity, &cv](const std::string& a_id, const ::ev::redis::subscriptions::Manager::Status& a_status) -> EV_REDIS_SUBSCRIPTIONS_DATA_POST_NOTIFY_CALLBACK {
                if ( ::ev::redis::subscriptions::Manager::Status::Unsubscribed == a_status ) {
                    // ... debug ...
                    CC_DEBUG_LOG_TRACE("job", "Job #" INT64_FMT " ~= unsubscribed from activity #" SIZET_FMT " channel '%s'",
                                       a_activity.sequence().bjid(), a_activity.index(), a_activity.rcid().c_str()
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
    
    // ... debug ...
    CC_DEBUG_LOG_TRACE("job", "Job #" INT64_FMT " ~= pusing activity #" SIZET_FMT " to beanstalkd",
                       a_activity.sequence().bjid(), a_activity.index()
    );
    
    Json::FastWriter jw; jw.omitEndingLineFeed();
    
    // ... submit job to beanstalkd queue ...
    PushJob(a_activity.payload()["tube"].asString(), jw.write(a_activity.payload()), a_activity.ttr());
    
    // ... debug ...
    CC_DEBUG_LOG_TRACE("job", "Job #" INT64_FMT " ~= activity #" SIZET_FMT " pushed to beanstalkd",
                       a_activity.sequence().bjid(), a_activity.index()
    );
}

/**
 * @brief Register an activity finalization.
 *
 * @param a_activity Activity info.
 * @param a_response Activity response.
 * @param o_next     Pre-filled next activity info ( payload, ttr and status to be set here, if there is another activity to load ).
 */
void casper::job::Sequencer::FinalizeActivity (casper::job::sequencer::Activity& a_activity, const Json::Value* a_response,
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
        // .. and set it as activity payload ...
        a_activity.SetPayload(attempt["response"]);
    } else {
        // ... else, payload MUST be already set, use it ...
        attempt["response"] = a_activity.payload();
    }
    
    // ... js.finalize_activity (sid INTEGER, id INTEGER, attempt INTEGER, response JSONB, status js.status) ...
    ss << "SELECT * FROM js.finalize_activity(";
    ss <<   a_activity.sequence().did() << ',' << a_activity.did() << ',' << a_activity.attempt();
    ss <<   ",'" << ::ev::postgresql::Request::SQLEscape(jw.write(attempt)) << "'";
    ss <<   ",'" << sequencer::Status::InProgress << "'";
    ss << ");";
  
    // ... debug ...
    CC_DEBUG_LOG_TRACE("job", "Job #" INT64_FMT " ~= registering activity #" SIZET_FMT " finalization",
                       a_activity.sequence().bjid(), a_activity.index()
    );
    
    // ... execute query ...
    ExecuteQueryAndWait(/* a_tracking         */ CC_TRACK_CALL(a_activity.sequence().bjid(), "FINALIZING ACTIVITY"),
                        /* a_query            */ ss.str(), /* a_expect   */ ExecStatusType::PGRES_TUPLES_OK,
                        /* a_success_callback */
                        [&o_next] (const Json::Value& a_value) {
                            // ... array is expected ...
                            if ( a_value.size() > 0 ) {
                                CC_WARNING_TODO("CJS: pick ttr");
                                o_next.Reset(sequencer::Status::Pending, /* a_payload */ a_value[0], /* a_ttr */ 0);
                            }
                        }
    );
    
    // ... debug ...
    CC_DEBUG_LOG_TRACE("job", "Job #" INT64_FMT " ~= activity #" SIZET_FMT " finalization registered",
                       a_activity.sequence().bjid(), a_activity.index()
    );
    
    // ... based on a_response, set activity status ...
    if ( nullptr == a_response ) {
        // ... first activity, then will launch ...
        o_next.SetStatus(casper::job::sequencer::Status::Done);
    } else {
        // ... next activity available?
        if ( false == o_next.payload().isNull() ) {
            // ... yes, but first check if previous activity succeeded ...
            const Json::Value status = (*a_response).get("status", "");
            const auto         m_it  = s_irj_teminal_status_map_.find(status.asCString());
            if ( s_irj_teminal_status_map_.end() == m_it ) {
                Json::Value response = Json::Value::null;
                // ... set standard 'failed' response ...
                SetFailedResponse(/* a_code */ 404, Json::Value("Invalid status '" + std::string(status.asCString()) + "'"), response);
                // ... invalid status - set internal error ...
                o_next.Reset(sequencer::Status::Failed, /* a_payload */ response, /* a_ttr */ 0);
            }
        } else { // true == o_next.payload().isNull()
            // ... no, we're done ...
            o_next.SetStatus(sequencer::Status::Done);
        }
    }

    // ... can't accept NotSet status ...
    assert(casper::job::sequencer::Status::NotSet != o_next.status());
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
    
    // ... debug ...
    CC_DEBUG_LOG_TRACE("job", "Job #" INT64_FMT " ~= track activity #" SIZET_FMT,
                       a_activity.sequence().bjid(), a_activity.index()
    );
    
    CC_WARNING_TODO("CJS: register an event in this thread so we can giveup tracking after ttr + n seconds");
    
    running_activities_[a_activity.rcid()] = new casper::job::sequencer::Activity(a_activity);
}

/**
 * @brief Track an activity.
 *
 * @param a_activity Activity info.
 */
void casper::job::Sequencer::TrackActivity (casper::job::sequencer::Activity* a_activity)
{
    CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);
    
    // ... debug ...
    CC_DEBUG_LOG_TRACE("job", "Job #" INT64_FMT " ~= track activity #" SIZET_FMT,
                       a_activity->sequence().bjid(), a_activity->index()
    );
    
    CC_WARNING_TODO("CJS: register an event in this thread so we can giveup tracking after ttr + n seconds");
    
    running_activities_[a_activity->rcid()] = a_activity;
}

/**
 * @brief Untrack an activity that is or will not be running.
 *
 * @param a_activity Activity info.
*/
void casper::job::Sequencer::UntrackActivity (const casper::job::sequencer::Activity& a_activity)
{
    CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);
 
    // ... debug ...
    CC_DEBUG_LOG_TRACE("job", "Job #" INT64_FMT " ~= untrack activity #" SIZET_FMT,
                       a_activity.sequence().bjid(), a_activity.index()
    );
    
    CC_WARNING_TODO("CJS: unregister then previously registered event");
    
    const auto it = running_activities_.find(a_activity.rcid());
    if ( running_activities_.end() != it ) {
        delete it->second;
        running_activities_.erase(it);
    }
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
void casper::job::Sequencer::ExecuteQueryAndWait (const casper::job::Sequencer::Tracking& a_tracking,
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
        
        NewTask([this, &a_query, &ex] () -> ::ev::Object* {
            
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
        throw casper::job::Sequencer::JumpErrorAlreadySet(a_tracking, /* a_code */ 500, LastError()["why"].asCString());
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

void casper::job::Sequencer::ReserveJob (const Json::Value& a_job) const
{
    
}

void casper::job::Sequencer::TrapUnhandledExceptions (const char* const a_type, const std::function<void()>& a_callback,
                                                      const casper::job::Sequencer::Tracking& a_tracking)
{
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
