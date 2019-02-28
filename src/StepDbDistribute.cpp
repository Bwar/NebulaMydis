/*******************************************************************************
 * Project:  NebulaMydis
 * @file     StepDbDistribute.cpp
 * @brief 
 * @author   Bwar
 * @date:    2018年3月3日
 * @note
 * Modify history:
 ******************************************************************************/
#include "StepDbDistribute.hpp"

namespace mydis
{

StepDbDistribute::StepDbDistribute(const neb::CJsonObject* pRedisNode)
    : m_oRedisNode(pRedisNode)
{
}

StepDbDistribute::~StepDbDistribute()
{
}

neb::E_CMD_STATUS StepDbDistribute::Emit(int iErrno, const std::string& strErrMsg, void* data)
{
    if (!SendRoundRobin("DBAGENT_R", GetContext()->GetCmd(), GetSequence(), GetContext()->GetMsgBody()))
    {
        LOG4_ERROR("SendRoundRobin(\"DBAGENT_R\") error!");
        Response(neb::ERR_DATA_TRANSFER, "SendRoundRobin(\"DBAGENT_R\") error!");
        return(neb::CMD_STATUS_FAULT);
    }
    return(neb::CMD_STATUS_RUNNING);
}

neb::E_CMD_STATUS StepDbDistribute::Callback(std::shared_ptr<neb::SocketChannel> pChannel,
                    const MsgHead& oInMsgHead, const MsgBody& oInMsgBody, void* data)
{
    LOG4_DEBUG("%s()", __FUNCTION__);
    MsgBody oOutMsgBody;
    neb::CJsonObject oRspJson;
    if (!oRspJson.Parse(oInMsgBody.data()))
    {
        LOG4_ERROR("oRspJson.Parse failed!");
    }
    if (m_oRedisNode.IsEmpty())
    {
        oOutMsgBody.set_data(oInMsgBody.data());
    }
    else
    {
        oRspJson.Add("redis_node", m_oRedisNode);
        oOutMsgBody.set_data(oRspJson.ToFormattedString());
    }
    if (!SendTo(GetContext()->GetChannel(), GetContext()->GetCmd() + 1, GetContext()->GetSeq(), oOutMsgBody))
    {
        return(neb::CMD_STATUS_FAULT);
    }
    return(neb::CMD_STATUS_COMPLETED);
}

neb::E_CMD_STATUS StepDbDistribute::Timeout()
{
    Response(neb::ERR_TIMEOUT, "\"DBAGENT_R\" timeout!");
    return(neb::CMD_STATUS_FAULT);
}

} /* namespace mydis */
