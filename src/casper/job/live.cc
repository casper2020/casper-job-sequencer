/**
* @file live.cc
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

#include "casper/job/live.h"

#include "cc/macros.h"
#include "cc/exception.h"

#ifdef __APPLE__
#pragma mark - casper::job::Recovery
#endif

const char* const casper::job::Live::s_tube_ = "sequencer-live";

/**
 * @brief Default constructor.
 *
 * param a_loggable_data
 * param a_config
 */
casper::job::Live::Live (const ev::Loggable::Data& a_loggable_data, const cc::easy::job::Job::Config& a_config)
    : casper::job::Sequencer(s_tube_, a_loggable_data, a_config)
{
    /* empty */
}

/**
 * @brief Destructor
 */
casper::job::Live::~Live ()
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
void casper::job::Live::Run (const uint64_t& a_id, const Json::Value& a_payload,
                             cc::easy::job::Job::Response& o_response)
{
    CC_DEBUG_FAIL_IF_NOT_AT_THREAD(thread_id_);
    
    //   {
    //      "id": "1",
    //      "ttr": 360,
    //      "validity": 500,
    //      "tube": "sequencer-live",
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

    const casper::job::sequencer::Tracking tracking = SEQUENCER_TRACK_CALL(a_id, "RUN JOB");

    // ... validate job payload ...
    CC_WARNING_TODO("CJS: validate job payload");
    
    // ... log status ...
    SEQUENCER_LOG_JOB(CC_JOB_LOG_LEVEL_INF, tracking.bjid_, CC_JOB_LOG_STEP_IN, "%s", "Validating");

    sequencer::Sequence* sequence = nullptr;
    
    try {
        
        //
        // NGINX-BROKER 'jobify' module awareness
        //
        const Json::Value* payload;
        Json::Value        origin = Json::Value(Json::ValueType::objectValue);
        if ( true == a_payload.isMember("body") && true == a_payload.isMember("headers") ) {
            // ... from nginx-broker 'jobify' module ...
            payload = &a_payload["body"];
            if ( true == a_payload["headers"].isArray() ) {
                for ( Json::ArrayIndex idx = 0 ; idx < a_payload["headers"].size() ; ++idx ) {
                    const auto& value = a_payload["headers"][idx];
                    if ( true == value.isString() && 0 == strncasecmp(value.asCString(), "User-Agent: ", 12 * sizeof(char) ) ) {
                        origin["user-agent"] = std::string(value.asCString() + ( 12 * sizeof(char) ));
                        break;
                    }
                }
                if ( true == a_payload.isMember("__nginx_broker__") ) {
                    origin["via"] = a_payload["__nginx_broker__"];
                }
            }
        } else {
            // ... direct from beanstalkd queue ...
            payload = &a_payload;
        }
        // ... no origin?
        if ( 0 == origin.getMemberNames().size() ) {
            origin = Json::Value::null;
        }
        
        // ... validate 'on_error' object ( if any ) ...
        const Json::Value& on_error = (*payload)["on_error"];
        if ( false == on_error.isNull() && false == on_error.isObject() ) {
            throw sequencer::JSONValidationException(tracking, "'on_error' is not a valid object!");
        }
        
        // ... register sequence and grab first activity ...
        try {

            // ... REDIS job id ...
            uint64_t rjnr = 0;
            if ( 1 != sscanf(a_payload["id"].asCString(), "" UINT64_FMT, &rjnr ) ) {
                throw sequencer::JSONValidationException(tracking, ( "Unable to parse job id from '" + a_payload["id"].asString() + "'!" ).c_str());
            }

            // ... create sequence from payload ...
            sequence = new sequencer::Sequence(
                                               /* a_source */ ( true == a_payload.isMember("body") && true == a_payload.isMember("headers")
                                                                ? sequencer::Sequence::Source::Jobification
                                                                : sequencer::Sequence::Source::Default
                                                              ),
                                               /* a_cid      */  config_.cluster(),
                                               /* a_iid      */  config_.instance(),
                                               /* a_bjid     */ tracking.bjid_,
                                               /* a_rsid     */ config_.service_id(),
                                               /* a_rjnr     */ rjnr,
                                               /* a_rjid     */ ( config_.service_id() + ":jobs:" + tube_ + ':' + a_payload["id"].asString() ),
                                               /* a_rcid     */ ( config_.service_id() + ':'      + tube_ + ':' + a_payload["id"].asString() ),
                                               /* a_origin   */ origin,
                                               /* a_on_error */ on_error
            );
            // ... register sequence ...
            auto first_activity = RegisterSequence(*sequence, *payload);
            try {
                // ... launch first activity ...
                o_response.code_ = LaunchActivity(tracking, first_activity, /* a_at_run */ true);
            } catch (const sequencer::JSONValidationException& a_jve) {
                // ... fallthrough to outer try - catch ...
                throw a_jve;
            } catch (const sequencer::V8ExpressionEvaluationException& a_v8eee) {
                // ... fallthrough to outer try - catch ...
                throw a_v8eee;
            } catch (...) {
                 // ... untrack activity ...
                 UntrackActivity(first_activity);
                 // ... rethrow ...
                 try {
                     ::cc::Exception::Rethrow(/* a_unhandled */ false,  tracking.file_.c_str(), tracking.line_, tracking.function_.c_str());
                } catch (::cc::Exception& a_cc_exception) {
                    // ... jump for common exception handling ...
                    throw sequencer::JumpErrorAlreadySet(tracking, /* o_code */ 500, a_cc_exception.what());
                }
            }
        } catch (const sequencer::Exception& a_exception) {
            // ... jump for common exception handling ...
            throw sequencer::JumpErrorAlreadySet(a_exception.tracking_, a_exception.code_, a_exception.what());
        }
        
    } catch (const sequencer::JumpErrorAlreadySet& a_exception) {

        // ... set response code ...
        if ( true == o_response.payload_.isNull() ) {
            o_response.code_ = SetError(a_exception.code_, /* a_i18n */ nullptr,
                                        /* a_error */ ::cc::easy::job::InternalError{
                                            /* code_ */ nullptr,
                                            /* why_ */ a_exception.what()
                                        }, o_response.payload_
            );
        } else {
            o_response.code_ = a_exception.code_;
        }

        Json::FastWriter fjw; fjw.omitEndingLineFeed();
        
        // ... log payload ...
        SEQUENCER_LOG_JOB(CC_JOB_LOG_LEVEL_ERR, tracking.bjid_, CC_JOB_LOG_STEP_DUMP, "%s",
                               fjw.write(a_payload).c_str()
        );

        // ... log exception ...
        SEQUENCER_LOG_JOB(CC_JOB_LOG_LEVEL_ERR, tracking.bjid_, CC_JOB_LOG_STEP_ERROR, "%s", a_exception.what());

        // ... log status ...
        if ( nullptr != sequence ) {
            SEQUENCER_LOG_SEQUENCE(CC_JOB_LOG_LEVEL_ERR, (*sequence), CC_JOB_LOG_STEP_STATUS, CC_JOB_LOG_COLOR(RED) "%s" CC_LOGS_LOGGER_RESET_ATTRS,
                                   "Rejected"
            );
        }
                
        // ... debug ...
        CC_JOB_LOG_TRACE(CC_JOB_LOG_LEVEL_DBG, "Job #" INT64_FMT " ~= ERROR JUMP =~\n\nORIGIN: %s:%d\nACTION: %s\n\%s\n",
                         a_exception.tracking_.bjid_,
                         a_exception.tracking_.function_.c_str(), a_exception.tracking_.line_,
                         a_exception.tracking_.action_.c_str(),
                         a_exception.what()
        );
    }
    
    // ... release temporary allocated objects ...
    if ( nullptr != sequence ) {
        delete sequence;
    }
    
    // ... if scheduled, then the response must be deferred ...
    if ( 200 == o_response.code_ ) {
        // ... this will remove job from beanstalkd queue, but keeps the redis status as is ( in-progress ) ...
        SetDeferred();
        // ... log status ...
        SEQUENCER_LOG_JOB(CC_JOB_LOG_LEVEL_INF, tracking.bjid_, CC_JOB_LOG_STEP_STATUS, CC_JOB_LOG_COLOR(GREEN) "%s" CC_LOGS_LOGGER_RESET_ATTRS, "Deferred");
    } else {
        // ... log status ...
        SEQUENCER_LOG_JOB(CC_JOB_LOG_LEVEL_INF, tracking.bjid_, CC_JOB_LOG_STEP_OUT, CC_JOB_LOG_COLOR(RED) "%s" CC_LOGS_LOGGER_RESET_ATTRS, "Rejected");
    }
}
