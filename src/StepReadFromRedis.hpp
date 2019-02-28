/*******************************************************************************
 * Project:  NebulaMydis
 * @file     StepReadFromRedis.hpp
 * @brief 
 * @author   Bwar
 * @date:    2018年3月1日
 * @note
 * Modify history:
 ******************************************************************************/
#ifndef MYDIS_STEPREADFROMREDIS_HPP_
#define MYDIS_STEPREADFROMREDIS_HPP_

#include "RedisStorageStep.hpp"

namespace mydis
{

class StepReadFromRedis: public RedisStorageStep
{
public:
    StepReadFromRedis(
                    const neb::Mydis::RedisOperate& oRedisOperate,
                    SessionRedisNode* pNodeSession,
                    bool bIsDataSet = false,
                    const neb::CJsonObject* pTableFields = NULL,
                    const std::string& strKeyField = "",
                    std::shared_ptr<neb::Step> pNextStep = nullptr);
    virtual ~StepReadFromRedis();

    virtual neb::E_CMD_STATUS Emit(int iErrno, const std::string& strErrMsg = "", void* data = NULL);

    virtual neb::E_CMD_STATUS Callback(
                    const redisAsyncContext *c,
                    int status,
                    redisReply* pReply);

protected:
    bool ReadReplyArray(redisReply* pReply);
    bool ReadReplyArrayWithDataSet(redisReply* pReply);
    bool ReadReplyArrayWithoutDataSet(redisReply* pReply);

    bool ReadReplyHash(redisReply* pReply);
    bool ReadReplyArrayForHashWithDataSet(redisReply* pReply);
    bool ReadReplyArrayForHashWithoutDataSet(redisReply* pReply);
    bool ReadReplyArrayForHgetallWithDataSet(redisReply* pReply);
    bool ReadReplyArrayForHgetallWithoutDataSet(redisReply* pReply);

private:
    neb::Mydis::RedisOperate m_oRedisOperate;
    bool m_bIsDataSet;
    neb::CJsonObject m_oTableFields;
    std::string m_strKeyField;
    std::string m_strMasterNode;
    std::string m_strSlaveNode;
    int m_iReadNum;     ///< 读数据次数，从一个节点读取失败会尝试从其他节点读取
    int m_iTableFieldNum;

public:
    SessionRedisNode* m_pNodeSession;
    std::shared_ptr<neb::Step> m_pNextStep;
};

} /* namespace mydis */

#endif /* MYDIS_STEPREADFROMREDIS_HPP_ */
