/*******************************************************************************
 * Project:  NebulaMydis
 * @file     StepReadFromRedisForWrite.hpp
 * @brief    读取redis中的数据，为下一步写入之用（适用于update dataset情形）
 * @author   Bwar
 * @date:    2018年3月3日
 * @note
 * Modify history:
 ******************************************************************************/
#ifndef MYDIS_STEPREADFROMREDISFORWRITE_HPP_
#define MYDIS_STEPREADFROMREDISFORWRITE_HPP_

#include "util/json/CJsonObject.hpp"
#include "RedisStorageStep.hpp"
#include "StepSendToDbAgent.hpp"
#include "StepWriteToRedis.hpp"

namespace mydis
{

class StepReadFromRedisForWrite: public RedisStorageStep
{
public:
    StepReadFromRedisForWrite(
                    const neb::Mydis& oMemOperate,
                    SessionRedisNode* pNodeSession,
                    const neb::CJsonObject& oTableFields,
                    const std::string& strKeyField = "");
    virtual ~StepReadFromRedisForWrite();

    virtual neb::E_CMD_STATUS Emit(int iErrno, const std::string& strErrMsg = "", void* data = NULL);

    virtual neb::E_CMD_STATUS Callback(
                    const redisAsyncContext *c,
                    int status,
                    redisReply* pReply);
protected:
    neb::E_CMD_STATUS ExecUpdate(bool bDbOnly = false);

private:
    neb::Mydis m_oMemOperate;
    neb::CJsonObject m_oTableFields;
    std::string m_strKeyField;
    std::string m_strMasterNode;
    std::string m_strSlaveNode;

    std::shared_ptr<SessionRedisNode> m_pRedisNodeSession;
    std::shared_ptr<neb::Step> m_pStepSendToDbAgent;
    std::shared_ptr<neb::Step> m_pStepWriteToRedis;
};

} /* namespace mydis */

#endif /* MYDIS_STEPREADFROMREDISFORWRITE_HPP_ */
