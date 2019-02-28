/*******************************************************************************
 * Project:  NebulaMydis
 * @file     StepSendToDbAgent.hpp
 * @brief 
 * @author   Bwar
 * @date:    2018年3月1日
 * @note
 * Modify history:
 ******************************************************************************/
#ifndef MYDIS_STEPSENDTODBAGENT_HPP_
#define MYDIS_STEPSENDTODBAGENT_HPP_

#include "DbStorageStep.hpp"
#include "StepWriteBackToRedis.hpp"
//#include "SessionSyncDbData.hpp"

namespace mydis
{

class StepSendToDbAgent: public DbStorageStep,
    public neb::DynamicCreator<StepSendToDbAgent, 
        neb::Mydis, std::shared_ptr<SessionRedisNode>, int, neb::CJsonObject*, std::string, neb::CJsonObject*>
{
public:
    /**
     * @brief 向DbAgent发出数据库操作请求
     * @param oMemOperate 存储操作请求数据
     * @param pNodeSession redis节点session
     * @param iRelative  redis数据结构与表之间的关系
     * @param pTableField 数据库表字段（json数组）
     * @param strKeyField redis数据结构键字段
     * @param pJoinField  redis数据结构串联字段
     */
    StepSendToDbAgent(
                    const neb::Mydis& oMemOperate,
                    std::shared_ptr<SessionRedisNode> pNodeSession = nullptr,
                    int iRelative = RELATIVE_TABLE,
                    const neb::CJsonObject* pTableField = nullptr,
                    const std::string& strKeyField = "",
                    const neb::CJsonObject* pJoinField = nullptr);
    virtual ~StepSendToDbAgent();

    virtual neb::E_CMD_STATUS Emit(int iErrno, const std::string& strErrMsg = "", void* data = NULL);

    virtual neb::E_CMD_STATUS Callback(
                    std::shared_ptr<neb::SocketChannel> pChannel,
                    const MsgHead& oMsgHead,
                    const MsgBody& oMsgBody,
                    void* data = NULL);

    virtual neb::E_CMD_STATUS Timeout();

protected:
    void WriteBackToRedis(const neb::Result& oRsp);

private:
    neb::Mydis m_oMemOperate;
    int m_iRelative;
    std::string m_strKeyField;
    neb::CJsonObject m_oTableField;
    neb::CJsonObject m_oJoinField;
    bool m_bFieldFilter;        ///< 是否需要筛选字段返回给请求方
    bool m_bNeedResponse;       ///< 是否需要响应请求方（如果是同时写redis和mysql，写redis已经给了响应，写mysql不再回响应）

public:
    std::shared_ptr<SessionRedisNode> m_pNodeSession;
    std::shared_ptr<neb::Step> m_pStepWriteBackToRedis;
};

} /* namespace mydis */

#endif /* MYDIS_STEPSENDTODBAGENT_HPP_ */
