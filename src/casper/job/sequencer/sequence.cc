/**
 * @file sequence.cc
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

#include "casper/job/sequencer/sequence.h"

/**
 * @brief Default constructor.
 *
 * @param a_source One of \link Sequence::Source \link.
 * @param a_cid    CLUSTER id.
 * @param a_bjid   BEANSTALKD job id ( for logging purposes ).
 * @param a_rsid   REDIS service id.
 * @param a_rjnr   REDIS job number.
 * @param a_rjid   REDIS job key.
 * @param a_rcid   REDIS job channel.
 */
casper::job::sequencer::Sequence::Sequence (const Sequence::Source& a_source,
                                            const uint64_t& a_cid, const int64_t& a_bjid,
                                            const std::string& a_rsid, const uint64_t& a_rjnr, const std::string& a_rjid, const std::string& a_rcid)
    : source_(a_source), cid_(a_cid), bjid_(a_bjid),
      rsid_(a_rsid), rjnr_(a_rjnr), rjid_(a_rjid), rcid_(a_rcid), count_(0)
{
    /* empty */
}

/**
 * @brief Default constructor.
 *
 * @param a_source One of \link Sequence::Source \link.
 * @param a_cid    CLUSTER id.
 * @param a_bjid   BEANSTALKD job id ( for logging purposes ).
 * @param a_rsid   REDIS service id.
 * @param a_rjnr   REDIS job number.
 * @param a_rjid   REDIS job key.
 * @param a_rcid   REDIS job channel.
 * @param a_djid   DB id ( form table js.sequences[id] as string ).
 */
casper::job::sequencer::Sequence::Sequence (const Sequence::Source& a_source,
                                            const uint64_t& a_cid, const int64_t& a_bjid,
                                            const std::string& a_rsid, const uint64_t& a_rjnr, const std::string& a_rjid, const std::string& a_rcid, const std::string& a_did)
    : source_(a_source), cid_(a_cid), bjid_(a_bjid),
      rsid_(a_rsid), rjnr_(a_rjnr), rjid_(a_rjid), rcid_(a_rcid), did_(a_did), count_(0)
{
    /* empty */
}

/**
 * @brief Copy constructor.
 *
 * @param a_sequence Object to copy.
 */
casper::job::sequencer::Sequence::Sequence (const casper::job::sequencer::Sequence& a_sequence)
{
    source_ = a_sequence.source_;
    cid_    = a_sequence.cid_;
    bjid_   = a_sequence.bjid_;
    rsid_   = a_sequence.rsid_;
    rjnr_   = a_sequence.rjnr_;
    rjid_   = a_sequence.rjid_;
    rcid_   = a_sequence.rcid_;
    did_    = a_sequence.did_;
    count_  = a_sequence.count_;
}
/**
 * @brief Destructor.
 */
casper::job::sequencer::Sequence::~Sequence ()
{
    /* empty */
}
