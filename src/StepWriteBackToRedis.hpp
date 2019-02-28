/*******************************************************************************
 * Project:  NebulaMydis
 * @file     StepWriteBackToRedis.hpp
 * @brief 
 * @author   Bwar
 * @date:    2018年3月1日
 * @note
 * Modify history:
 ******************************************************************************/
#ifndef MYDIS_STEPWRITEBACKTOREDIS_HPP_
#define MYDIS_STEPWRITEBACKTOREDIS_HPP_

#include "RedisStorageStep.hpp"
#include "StepSetTtl.hpp"

namespace mydis
{

class StepWriteBackToRedis: public RedisStorageStep,
    public neb::DynamicCreator<StepWriteBackToRedis,
        neb::Mydis, std::shared_ptr<SessionRedisNode>, int, std::string, neb::CJsonObject*>
{
public:
    StepWriteBackToRedis(
                    const neb::Mydis& oMemOperate,
                    std::shared_ptr<SessionRedisNode> pNodeSession,
                    int iRelative = RELATIVE_TABLE,
                    const std::string& strKeyField = "",
                    const neb::CJsonObject* pJoinField = NULL);
    virtual ~StepWriteBackToRedis();

    virtual neb::E_CMD_STATUS Emit(int iErrno, const std::string& strErrMsg = "", void* data = NULL)
    {
        return(neb::CMD_STATUS_COMPLETED);
    }

    virtual neb::E_CMD_STATUS Emit(const neb::Result& oRsp);

    virtual neb::E_CMD_STATUS Callback(
                    const redisAsyncContext *c,
                    int status,
                    redisReply* pReply);

protected:
    bool MakeCmdWithJoin(const neb::Result& oRsp);
    bool MakeCmdWithDataSet(const neb::Result& oRsp);
    bool MakeCmdWithoutDataSet(const neb::Result& oRsp);
    bool MakeCmdForHashWithDataSet(const neb::Result& oRsp);
    bool MakeCmdForHashWithoutDataSet(const neb::Result& oRsp);
    bool MakeCmdForHashWithoutDataSetWithField(const neb::Result& oRsp);
    bool MakeCmdForHashWithoutDataSetWithoutField(const neb::Result& oRsp);

private:
    neb::Mydis m_oMemOperate;
    int m_iRelative;
    std::string m_strKeyField;
    neb::CJsonObject m_oJoinField;
    std::string m_strMasterNode;
    std::string m_strSlaveNode;

public:
    std::shared_ptr<SessionRedisNode> m_pNodeSession;
    std::shared_ptr<neb::Step> m_pStepSetTtl;
};

} /* namespace mydis */

#endif /* MYDIS_STEPWRITEBACKTOREDIS_HPP_ */
