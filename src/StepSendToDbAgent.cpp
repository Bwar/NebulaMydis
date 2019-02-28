/*******************************************************************************
 * Project:  NebulaMydis
 * @file     StepSendToDbAgent.cpp
 * @brief 
 * @author   Bwar
 * @date:    2018年3月1日
 * @note
 * Modify history:
 ******************************************************************************/
#include <sstream>
#include "StepSendToDbAgent.hpp"

namespace mydis
{

StepSendToDbAgent::StepSendToDbAgent(
                const neb::Mydis& oMemOperate,
                std::shared_ptr<SessionRedisNode> pNodeSession,
                int iRelative,
                const neb::CJsonObject* pTableField,
                const std::string& strKeyField,
                const neb::CJsonObject* pJoinField)
    : m_oMemOperate(oMemOperate),
      m_iRelative(iRelative), m_strKeyField(strKeyField), m_oTableField(pTableField), m_oJoinField(pJoinField),
      m_bFieldFilter(false), m_bNeedResponse(true),
      m_pNodeSession(pNodeSession), m_pStepWriteBackToRedis(NULL)
{
}

StepSendToDbAgent::~StepSendToDbAgent()
{
}

neb::E_CMD_STATUS StepSendToDbAgent::Emit(int iErrno, const std::string& strErrMsg, void* data)
{
    /*
    std::ostringstream oss;
    oss << "mydis::SessionSyncDbData-" << m_oMemOperate.db_operate().table_name();
    std::shared_ptr<neb::Session> pSessionSync = GetSession(oss.str());
    if (neb::Mydis::DbOperate::SELECT != m_oMemOperate.db_operate().query_type()
                    && nullptr != pSessionSync)
    {
        if ((std::dynamic_pointer_cast<SessionSyncDbData>(pSessionSync))->IsSendDelay(m_oMemOperate)
            && (std::dynamic_pointer_cast<SessionSyncDbData>(pSessionSync))->SyncData(m_oMemOperate))
        {
            return(neb::CMD_STATUS_COMPLETED);
        }
    }
    */

    MsgHead oOutMsgHead;
    MsgBody oOutMsgBody;
    if (neb::Mydis::DbOperate::SELECT != m_oMemOperate.db_operate().query_type()
                    && m_oMemOperate.has_redis_operate())
    {
        m_bNeedResponse = false;
    }
    LOG4_TRACE("%s(m_bNeedResponse = %d)", __FUNCTION__, m_bNeedResponse);
    if (RELATIVE_DATASET == m_iRelative
                    && neb::Mydis::DbOperate::SELECT == m_oMemOperate.db_operate().query_type())
    {
        for (int i = 0; i < m_oMemOperate.db_operate().fields_size(); ++i)
        {
            if ((m_oTableField(i) != m_oMemOperate.db_operate().fields(i).col_name())
                            && (m_oMemOperate.db_operate().fields(i).col_as().length() > 0
                                            && m_oTableField(i) != m_oMemOperate.db_operate().fields(i).col_as()))
            {
                m_bFieldFilter = true;
                break;
            }
        }
        // 客户端请求表的部分字段，但redis需要所有字段来回写，故修改向数据库的请求数据，待返回结果集再筛选部分字段回复客户端，所有字段用来回写redis
        if (m_bFieldFilter)
        {
            neb::Mydis oMemOperate = m_oMemOperate;
            neb::Mydis::DbOperate* oDbOper = oMemOperate.mutable_db_operate();
            oDbOper->clear_fields();
            for (int i = 0; i < m_oTableField.GetArraySize(); ++i)
            {
                neb::Field* pField = oDbOper->add_fields();
                pField->set_col_name(m_oTableField(i));
            }
            oOutMsgBody.set_data(oMemOperate.SerializeAsString());
        }
        else
        {
            oOutMsgBody.set_data(m_oMemOperate.SerializeAsString());
        }
    }
    else
    {
        oOutMsgBody.set_data(m_oMemOperate.SerializeAsString());
    }

    if (m_oMemOperate.db_operate().SELECT == m_oMemOperate.db_operate().query_type())
    {
        if (!SendRoundRobin("DBAGENT_R", neb::CMD_REQ_STORATE, GetSequence(), oOutMsgBody))
        {
            LOG4_ERROR("SendToNext(\"DBAGENT_R\") error!");
            Response(neb::ERR_DATA_TRANSFER, "SendToNext(\"DBAGENT_R\") error!");
            return(neb::CMD_STATUS_FAULT);
        }
    }
    else
    {
        if (!SendRoundRobin("DBAGENT_W", neb::CMD_REQ_STORATE, GetSequence(), oOutMsgBody))
        {
            LOG4_ERROR("SendToNext(\"DBAGENT_W\") error!");
            Response(neb::ERR_DATA_TRANSFER, "SendToNext(\"DBAGENT_W\") error!");
            return(neb::CMD_STATUS_FAULT);
        }
    }
    return(neb::CMD_STATUS_RUNNING);
}

neb::E_CMD_STATUS StepSendToDbAgent::Callback(std::shared_ptr<neb::SocketChannel> pChannel,
                const MsgHead& oMsgHead, const MsgBody& oMsgBody, void* data)
{
    LOG4_TRACE("%s()", __FUNCTION__);
    MsgBody oOutMsgBody;
    neb::Result oRsp;
    if (!oRsp.ParseFromString(oMsgBody.data()))
    {
        LOG4_ERROR("neb::Result oRsp.ParseFromString() failed!");
        if (m_bNeedResponse)
        {
            oOutMsgBody.set_data(oMsgBody.data());
            if (!SendTo(GetContext()->GetChannel(), GetContext()->GetCmd(), GetContext()->GetSeq(), oOutMsgBody))
            {
                return(neb::CMD_STATUS_FAULT);
            }
        }
        return(neb::CMD_STATUS_FAULT);
    }

    WriteBackToRedis(oRsp);

    if (m_bNeedResponse)
    {
        if (m_bFieldFilter)     // 需筛选回复字段
        {
            neb::Result oRspToClient;
            oRspToClient.set_err_no(oRsp.err_no());
            oRspToClient.set_err_msg(oRsp.err_msg());
            std::vector<int> vecColForClient;
            std::vector<int>::iterator iter;
            for (int i = 0; i < m_oMemOperate.db_operate().fields_size(); ++i)
            {
                for (int j = 0; j < m_oTableField.GetArraySize(); ++j)
                {
                    if ((m_oTableField(j) == m_oMemOperate.db_operate().fields(i).col_name())
                                || (m_oTableField(j) == m_oMemOperate.db_operate().fields(i).col_as()))
                    {
                        vecColForClient.push_back(j);
                        break;
                    }
                }
            }
            for(int i = 0; i < oRsp.record_data_size(); i++)
            {
                neb::Record* pRecord = oRspToClient.add_record_data();
                for (iter = vecColForClient.begin(); iter != vecColForClient.end(); ++iter)
                {
                    neb::Field* pField = pRecord->add_field_info();
                    pField->set_col_value(oRsp.record_data(i).field_info(*iter).col_value());
                }
            }
            oOutMsgBody.set_data(oRspToClient.SerializeAsString());
        }
        else
        {
            oOutMsgBody.set_data(oMsgBody.data());
        }
        if (!SendTo(GetContext()->GetChannel(), GetContext()->GetCmd(), GetContext()->GetSeq(), oOutMsgBody))
        {
            return(neb::CMD_STATUS_FAULT);
        }
    }
    else    // 无需回复请求方
    {
        /*
        if (neb::ERR_OK != oRsp.err_no())
        {
            std::ostringstream oss;
            oss << "mydis::SessionSyncDbData-" << m_oMemOperate.db_operate().table_name();
            std::shared_ptr<neb::Session> pSessionSync = GetSession(oss.str());
            if (nullptr == pSessionSync)
            {
                pSessionSync = MakeSharedSession("mydis::SessionSyncDbData", m_oMemOperate.db_operate().table_name(),
                                std::string(GetWorkPath() + "/conf/Proxy/"), 60.0);
                if (nullptr == pSessionSync)
                {
                    return(neb::CMD_STATUS_FAULT);
                }
                (std::dynamic_pointer_cast<SessionSyncDbData>(pSessionSync))->ScanSyncData();
            }
            if (neb::Mydis::DbOperate::SELECT != m_oMemOperate.db_operate().query_type()
                            && ((oRsp.err_no() >= 2001 && oRsp.err_no() <= 2018)
                            || (oRsp.err_no() >= 2038 && oRsp.err_no() <= 2046)))
            {
                if ((std::dynamic_pointer_cast<SessionSyncDbData>(pSessionSync))->SyncData(m_oMemOperate))
                {
                    LOG4_WARNING("%d: %s.    sync data", oRsp.err_no(), oRsp.err_msg().c_str());
                }
            }
            LOG4_ERROR("%d: %s", oRsp.err_no(), oRsp.err_msg().c_str());
            return(neb::CMD_STATUS_FAULT);
        }
        */
    }

    if (oRsp.total_count() == oRsp.current_count())
    {
        return(neb::CMD_STATUS_COMPLETED);
    }
    return(neb::CMD_STATUS_RUNNING);
}

neb::E_CMD_STATUS StepSendToDbAgent::Timeout()
{
    Response(neb::ERR_TIMEOUT, "read from db or write to db timeout!");
    return(neb::CMD_STATUS_FAULT);
}

void StepSendToDbAgent::WriteBackToRedis(const neb::Result& oRsp)
{
    if (m_oMemOperate.has_redis_operate()
                    && (neb::Mydis::RedisOperate::T_READ == m_oMemOperate.redis_operate().op_type())
                    && (neb::ERR_OK == oRsp.err_no()))
    {
        m_pStepWriteBackToRedis = MakeSharedStep("mydis::StepWriteBackToRedis", m_oMemOperate,
                        m_pNodeSession, m_iRelative, m_strKeyField, &m_oJoinField);
        if (nullptr == m_pStepWriteBackToRedis)
        {
            return;
        }
        (std::dynamic_pointer_cast<StepWriteBackToRedis>(m_pStepWriteBackToRedis))->Emit(oRsp);
    }
}

} /* namespace mydis */
