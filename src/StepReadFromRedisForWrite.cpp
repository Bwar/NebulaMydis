/*******************************************************************************
 * Project:  NebulaMydis
 * @file     StepReadFromRedisForWrite.cpp
 * @brief 
 * @author   Bwar
 * @date:    2018年3月3日
 * @note
 * Modify history:
 ******************************************************************************/
#include "StepReadFromRedisForWrite.hpp"

namespace mydis
{

StepReadFromRedisForWrite::StepReadFromRedisForWrite(
                const neb::Mydis& oMemOperate,
                SessionRedisNode* pNodeSession,
                const neb::CJsonObject& oTableFields,
                const std::string& strKeyField)
    : m_oMemOperate(oMemOperate),
      m_oTableFields(oTableFields), m_strKeyField(strKeyField),
      m_pRedisNodeSession(pNodeSession), m_pStepSendToDbAgent(nullptr), m_pStepWriteToRedis(nullptr)
{
}

StepReadFromRedisForWrite::~StepReadFromRedisForWrite()
{
}

neb::E_CMD_STATUS StepReadFromRedisForWrite::Emit(int iErrno, const std::string& strErrMsg, void* data)
{
    LOG4_TRACE("%s()", __FUNCTION__);
    if (!m_pRedisNodeSession)
    {
        return(neb::CMD_STATUS_FAULT);
    }

    bool bGetRedisNode;
    if (m_oMemOperate.redis_operate().hash_key().size() > 0)
    {
        bGetRedisNode = m_pRedisNodeSession->GetRedisNode(m_oMemOperate.redis_operate().hash_key(), m_strMasterNode, m_strSlaveNode);
    }
    else
    {
        bGetRedisNode = m_pRedisNodeSession->GetRedisNode(m_oMemOperate.redis_operate().key_name(), m_strMasterNode, m_strSlaveNode);
    }
    if (!bGetRedisNode)
    {
        Response(neb::ERR_REDIS_NODE_NOT_FOUND, "redis node not found!");
        return(neb::CMD_STATUS_FAULT);
    }

    SetCmd(m_oMemOperate.redis_operate().redis_cmd_read());
    Append(m_oMemOperate.redis_operate().key_name());
    for (int i = 0; i < m_oMemOperate.redis_operate().fields_size(); ++i)
    {
        if (m_oMemOperate.redis_operate().fields(i).col_name().size() > 0)
        {
            Append(m_oMemOperate.redis_operate().fields(i).col_name());
        }
        if (m_oMemOperate.redis_operate().fields(i).col_value().size() > 0)
        {
            Append(m_oMemOperate.redis_operate().fields(i).col_value());
        }
    }

    if (SendTo(m_strMasterNode))
    {
        return(neb::CMD_STATUS_RUNNING);
    }
    else
    {
        Response(neb::ERR_REGISTERCALLBACK_REDIS, "SendTo redis node error!");
        LOG4_ERROR("SendTo(%s) error!", m_strMasterNode.c_str());
        return(neb::CMD_STATUS_FAULT);
    }
}

neb::E_CMD_STATUS StepReadFromRedisForWrite::Callback(const redisAsyncContext *c, int status, redisReply* pReply)
{
    LOG4_TRACE("%s()", __FUNCTION__);
    char szErrMsg[256] = {0};
    if (REDIS_OK != status)
    {
        snprintf(szErrMsg, sizeof(szErrMsg), "redis %s cmd status %d!", m_strMasterNode.c_str(), status);
        Response(neb::ERR_REDIS_CMD, szErrMsg);
        return(neb::CMD_STATUS_FAULT);
    }
    if (NULL == pReply)
    {
        snprintf(szErrMsg, sizeof(szErrMsg), "redis %s error %d: %s!", m_strMasterNode.c_str(), c->err, c->errstr);
        Response(neb::ERR_REDIS_CMD, szErrMsg);
        return(neb::CMD_STATUS_FAULT);
    }
    LOG4_TRACE("redis reply->type = %d", pReply->type);
    if (REDIS_REPLY_ERROR == pReply->type)
    {
        snprintf(szErrMsg, sizeof(szErrMsg), "redis %s error %d: %s!", m_strSlaveNode.c_str(), pReply->type, pReply->str);
        Response(neb::ERR_REDIS_CMD, szErrMsg);
        return(neb::CMD_STATUS_FAULT);
    }
    if (REDIS_REPLY_NIL == pReply->type) // redis中数据为空，只需update db中的数据
    {
        return(ExecUpdate(true));
    }

    // 从redis中读到数据
    if (REDIS_REPLY_STRING == pReply->type) // dataset返回的结果仅有 REDIS_REPLY_STRING 一种可能
    {
        neb::Field* pDatasetField = NULL;
        neb::Record oRecord;
        int iRecoredFieldSize = 0;
        int iDictFieldSize = 0;
        if (0 == m_oMemOperate.redis_operate().fields_size())   // field_size()只会有0和1两种情况，在CmdMydis中已做验证
        {
            pDatasetField = m_oMemOperate.mutable_redis_operate()->add_fields();
        }
        else
        {
            pDatasetField = m_oMemOperate.mutable_redis_operate()->mutable_fields(0);
        }
        if (!oRecord.ParseFromArray(pReply->str, pReply->len))
        {
            snprintf(szErrMsg, sizeof(szErrMsg), "failed to parse redis dataset record \"%s\"", pReply->str);
            LOG4_ERROR("%d: %s!", neb::ERR_UNEXPECTED_REDIS_REPLY, szErrMsg);
            Response(neb::ERR_UNEXPECTED_REDIS_REPLY, szErrMsg);
            return(neb::CMD_STATUS_FAULT);
        }
        if (oRecord.field_info_size() == 0)
        {
            LOG4_ERROR("%d: can not update empty redis dataset record!", neb::ERR_UNEXPECTED_REDIS_REPLY);
            Response(neb::ERR_UNEXPECTED_REDIS_REPLY, "can not update empty redis dataset record");
            return(neb::CMD_STATUS_FAULT);
        }
        iRecoredFieldSize = oRecord.field_info_size();
        iDictFieldSize = m_oTableFields.GetArraySize();
        if (iRecoredFieldSize < iDictFieldSize)
        {
            LOG4_TRACE("iRecoredFieldSize = %d, iDictFieldSize = %d", iRecoredFieldSize, iDictFieldSize);
            neb::Field* pField = NULL;
            for (int i = 0; i < iDictFieldSize - iRecoredFieldSize; ++i)
            {
                pField = oRecord.add_field_info();
                pField->set_col_value("");
            }
        }
        for (int j = 0; j < m_oMemOperate.db_operate().fields_size(); ++j)
        {
            for (int k = 0; k < m_oTableFields.GetArraySize(); ++k)
            {
                if (m_oMemOperate.db_operate().fields(j).col_name() == m_oTableFields(k)
                                || m_oMemOperate.db_operate().fields(j).col_as() == m_oTableFields(k))
                {
                    oRecord.mutable_field_info(k)->set_col_value(m_oMemOperate.db_operate().fields(j).col_value());
                }
            }
        }
        pDatasetField->set_col_value(oRecord.SerializeAsString());
        return(ExecUpdate());
    }
    else
    {
        snprintf(szErrMsg, sizeof(szErrMsg), "unexprected redis reply type %d for update redis dataset!", pReply->type);
        Response(neb::ERR_UNEXPECTED_REDIS_REPLY, szErrMsg);
        return(neb::CMD_STATUS_FAULT);
    }
    return(neb::CMD_STATUS_FAULT);
}

neb::E_CMD_STATUS StepReadFromRedisForWrite::ExecUpdate(bool bDbOnly)
{
    if (bDbOnly)
    {
        m_oMemOperate.clear_redis_operate();
        m_pStepSendToDbAgent = MakeSharedStep("mydis::StepSendToDbAgent", m_oMemOperate,
                        m_pRedisNodeSession, RELATIVE_DATASET, &m_oTableFields, m_strKeyField);
        if (m_pStepSendToDbAgent != nullptr)
        {
            if (neb::CMD_STATUS_RUNNING == m_pStepSendToDbAgent->Emit(neb::ERR_OK))
            {
                LOG4_TRACE("m_pStepSendToDbAgent running");
                return(neb::CMD_STATUS_COMPLETED);
            }
            else
            {
                return(neb::CMD_STATUS_COMPLETED);
            }
        }
        else
        {
            Response(neb::ERR_NEW, "new m_pStepSendToDbAgent!");
            return(neb::CMD_STATUS_FAULT);
        }
    }
    else
    {
        m_pStepSendToDbAgent = MakeSharedStep("StepSendToDbAgent", m_oMemOperate,
                        m_pRedisNodeSession, RELATIVE_DATASET, &m_oTableFields, m_strKeyField);
        if (nullptr == m_pStepSendToDbAgent)
        {
            Response(neb::ERR_NEW, "malloc space for StepSendToDbAgent error!");
            return(neb::CMD_STATUS_FAULT);
        }

        m_pStepWriteToRedis = MakeSharedStep("StepWriteToRedis", m_oMemOperate.redis_operate(), m_pRedisNodeSession, m_pStepSendToDbAgent);
        if (nullptr == m_pStepWriteToRedis)
        {
            Response(neb::ERR_NEW, "malloc space for StepWriteToRedis error!");
            return(neb::CMD_STATUS_FAULT);
        }
        if (neb::CMD_STATUS_RUNNING == m_pStepWriteToRedis->Emit(neb::ERR_OK))
        {
            return(neb::CMD_STATUS_COMPLETED);
        }
        return(neb::CMD_STATUS_FAULT);
    }
}

} /* namespace mydis */
