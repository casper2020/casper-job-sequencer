/**
 * @file activity.cc
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

#include "casper/job/sequencer/activity.h"

/**
 * @brief Default constructor.
 *
 * @param a_sequence
 * @param a_did
 * @param a_index
 * @param a_attempt
 */
casper::job::sequencer::Activity::Activity (const Sequence& a_sequence, const std::string& a_did, const size_t& a_index, const size_t& a_attempt)
 : sequence_(a_sequence)
{
    did_      = a_did;
    index_    = a_index;
    payload_  = Json::Value::null;
    attempt_  = 0;
    rjnr_     = 0;
    rjid_     = "";
    rcnm_     = "";
    rcid_     = "";
    status_   = casper::job::sequencer::Status::NotSet;
    validity_ = 0;
    ttr_      = 0;
}

/**
 * @brief Copy constructor.
 *
 * @param a_activity Object to copy.
 */
casper::job::sequencer::Activity::Activity (const casper::job::sequencer::Activity& a_activity)
    : sequence_(a_activity.sequence_)
{
    did_         = a_activity.did_;
    index_       = a_activity.index_;
    payload_     = a_activity.payload_;
    attempt_     = a_activity.attempt_;
    rjnr_        = a_activity.rjnr_;
    rjid_        = a_activity.rjid_;
    rcnm_        = a_activity.rcnm_;
    rcid_        = a_activity.rcid_;
    status_      = a_activity.status_;
    validity_    = a_activity.validity_;
    ttr_         = a_activity.ttr_;
    abort_expr_  = a_activity.abort_expr_;
}

/**
 * @brief Destructor.
 */
casper::job::sequencer::Activity::~Activity ()
{
    payload_.clear();
}
