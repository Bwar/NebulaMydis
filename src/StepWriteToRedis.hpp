/*******************************************************************************
 * Project:  NebulaMydis
 * @file     StepWriteToRedis.hpp
 * @brief 
 * @author   Bwar
 * @date:    2018年3月1日
 * @note
 * Modify history:
 ******************************************************************************/
#ifndef MYDIS_STEPWRITETOREDIS_HPP_
#define MYDIS_STEPWRITETOREDIS_HPP_

#include "RedisStorageStep.hpp"
#include "StepSetTtl.hpp"

namespace mydis
{

class StepWriteToRedis: public RedisStorageStep
{
public:
    StepWriteToRedis(
                    const neb::Mydis::RedisOperate& oRedisOperate,
                    SessionRedisNode* pNodeSession, std::shared_ptr<neb::Step> pNextStep = nullptr);
    virtual ~StepWriteToRedis();

    virtual neb::E_CMD_STATUS Emit(int iErrno, const std::string& strErrMsg = "", void* data = NULL);

    virtual neb::E_CMD_STATUS Callback(
                    const redisAsyncContext *c,
                    int status,
                    redisReply* pReply);

protected:
    bool ReadReplyArrayForHashWithoutDataSet(redisReply* pReply);
    bool ReadReplyArrayWithoutDataSet(redisReply* pReply);

private:
    neb::Mydis::RedisOperate m_oRedisOperate;
    std::string m_strMasterNode;
    std::string m_strSlaveNode;

    std::shared_ptr<SessionRedisNode> m_pNodeSession;
    std::shared_ptr<neb::Step> m_pStepSetTtl;
};

} /* namespace mydis */

#endif /* MYDIS_STEPWRITETOREDIS_HPP_ */
