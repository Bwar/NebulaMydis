/*******************************************************************************
 * Project:  NebulaMydis
 * @file     StepSyncToDb.cpp
 * @brief 
 * @author   Bwar
 * @date:    2018年7月15日
 * @note
 * Modify history:
 ******************************************************************************/
#include "StepSyncToDb.hpp"

namespace mydis
{

StepSyncToDb::StepSyncToDb(SessionSyncDbData* pSyncSession, const MsgHead& oInMsgHead, const MsgBody& oInMsgBody)
    : m_pSyncSession(pSyncSession), m_oMsgHead(oInMsgHead), m_oMsgBody(oInMsgBody)
{
}

StepSyncToDb::~StepSyncToDb()
{
}

neb::E_CMD_STATUS StepSyncToDb::Emit(int iErrno, const std::string& strErrMsg, void* data)
{
    LOG4_TRACE("%s()", __FUNCTION__);
    m_oMsgHead.set_seq(GetSequence());
    if (!SendRoundRobin("DBAGENT_W", m_oMsgHead.cmd(), m_oMsgHead.seq(), m_oMsgBody))
    {
        LOG4_ERROR("SendRoundRobin(\"DBAGENT_W\") error!");
        m_pSyncSession->GoBack(m_oMsgHead, m_oMsgBody);
        return(neb::CMD_STATUS_FAULT);
    }
    return(neb::CMD_STATUS_RUNNING);
}

neb::E_CMD_STATUS StepSyncToDb::Callback(std::shared_ptr<neb::SocketChannel> pChannel,
                int32 iCmd, uint32 uiSeq, const MsgBody& oInMsgBody, void* data)
{
    LOG4_TRACE("%s()", __FUNCTION__);
    neb::Result oRsp;
    if (!oRsp.ParseFromString(oInMsgBody.data()))
    {
        LOG4_ERROR("neb::Result oRsp.ParseFromString() failed!");
        return(neb::CMD_STATUS_FAULT);
    }

    if (neb::ERR_OK != oRsp.err_no())
    {
        LOG4_ERROR("%d: %s", oRsp.err_no(), oRsp.err_msg().c_str());
        //if (neb::Mydis::DbOperate::SELECT != oMemOperate.db_operate().query_type()
        if ((oRsp.err_no() >= 2001 && oRsp.err_no() <= 2018)
                        || (oRsp.err_no() >= 2038 && oRsp.err_no() <= 2046))
        {   // 由于连接方面原因数据写失败，先写入文件，等服务从故障中恢复后再自动重试
            m_pSyncSession->GoBack(m_oMsgHead, m_oMsgBody);
            return(neb::CMD_STATUS_FAULT);
        }
    }

    if (m_pSyncSession->GetSyncData(m_oMsgHead, m_oMsgBody))
    {
        return(Emit(neb::ERR_OK));
    }
    return(neb::CMD_STATUS_COMPLETED);
}

neb::E_CMD_STATUS StepSyncToDb::Timeout()
{
    return(neb::CMD_STATUS_FAULT);
}

} /* namespace mydis */
