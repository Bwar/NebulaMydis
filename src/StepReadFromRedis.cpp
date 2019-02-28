/*******************************************************************************
 * Project:  NebulaMydis
 * @file     StepReadFromRedis.cpp
 * @brief 
 * @author   Bwar
 * @date:    2018年3月1日
 * @note
 * Modify history:
 ******************************************************************************/
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include "StepReadFromRedis.hpp"

namespace mydis
{

StepReadFromRedis::StepReadFromRedis(
                const neb::Mydis::RedisOperate& oRedisOperate,
                SessionRedisNode* pNodeSession,
                bool bIsDataSet,
                const neb::CJsonObject* pTableFields,
                const std::string& strKeyField,
                std::shared_ptr<neb::Step> pNextStep)
    : RedisStorageStep(pNextStep),
      m_oRedisOperate(oRedisOperate),
      m_bIsDataSet(bIsDataSet), m_oTableFields(pTableFields), m_strKeyField(strKeyField),
      m_iReadNum(0), m_iTableFieldNum(0),
      m_pNodeSession(pNodeSession), m_pNextStep(pNextStep)
{
    m_iTableFieldNum = m_oTableFields.GetArraySize();
}

StepReadFromRedis::~StepReadFromRedis()
{
}

neb::E_CMD_STATUS StepReadFromRedis::Emit(int iErrno, const std::string& strErrMsg, void* data)
{
    LOG4_TRACE("%s()", __FUNCTION__);
    if (!m_pNodeSession)
    {
        return(neb::CMD_STATUS_FAULT);
    }

    if (0 == m_iReadNum)
    {
        bool bGetRedisNode;
        if (m_oRedisOperate.hash_key().size() > 0)
        {
            bGetRedisNode = m_pNodeSession->GetRedisNode(m_oRedisOperate.hash_key(), m_strMasterNode, m_strSlaveNode);
        }
        else
        {
            bGetRedisNode = m_pNodeSession->GetRedisNode(m_oRedisOperate.key_name(), m_strMasterNode, m_strSlaveNode);
        }
        if (!bGetRedisNode)
        {
            Response(neb::ERR_REDIS_NODE_NOT_FOUND, "redis node not found!");
            return(neb::CMD_STATUS_FAULT);
        }
    }

    SetCmd(m_oRedisOperate.redis_cmd_read());
    Append(m_oRedisOperate.key_name());
    for (int i = 0; i < m_oRedisOperate.fields_size(); ++i)
    {
        if (m_oRedisOperate.fields(i).col_name().size() > 0)
        {
            Append(m_oRedisOperate.fields(i).col_name());
        }
        if (m_oRedisOperate.fields(i).col_value().size() > 0)
        {
            Append(m_oRedisOperate.fields(i).col_value());
        }
    }

    if (0 == m_iReadNum)  // 从备Redis节点读取
    {
        if (SendTo(m_strSlaveNode))
        {
            ++m_iReadNum;
            return(neb::CMD_STATUS_RUNNING);
        }
    }
    else    // 从主Redis节点读取（说明第一次从备Redis节点读取失败）
    {
        if (SendTo(m_strMasterNode))
        {
            ++m_iReadNum;
            return(neb::CMD_STATUS_RUNNING);
        }
    }
    return(neb::CMD_STATUS_FAULT);
}

neb::E_CMD_STATUS StepReadFromRedis::Callback(const redisAsyncContext *c, int status, redisReply* pReply)
{
    LOG4_TRACE("%s()", __FUNCTION__);
    char szErrMsg[256] = {0};
    if (REDIS_OK != status)
    {
        if (0 == m_iReadNum)
        {
            LOG4_WARNING("redis %s cmd status %d!", m_strSlaveNode.c_str(), status);
            return(Emit(neb::ERR_OK));
        }
        snprintf(szErrMsg, sizeof(szErrMsg), "redis %s cmd status %d!", m_strMasterNode.c_str(), status);
        Response(neb::ERR_REDIS_CMD, szErrMsg);
        return(neb::CMD_STATUS_FAULT);
    }
    if (NULL == pReply)
    {
        if (0 == m_iReadNum)
        {
            LOG4_WARNING("redis %s error %d: %s!", m_strSlaveNode.c_str(), c->err, c->errstr);
            return(Emit(neb::ERR_OK));
        }
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
    if (REDIS_REPLY_NIL == pReply->type)
    {
        if (m_pNextStep)    // redis中数据为空，有下一步（通常是再尝试从DB中读）则执行下一步
        {
            if (!m_pNextStep->Emit())
            {
                Response(neb::ERR_REDIS_NIL_AND_DB_FAILED,
                                "redis result set is nil and send to dbagent failed!");
                return(neb::CMD_STATUS_FAULT);
            }
            return(neb::CMD_STATUS_COMPLETED);
        }
        else                // 只读redis，redis的结果又为空
        {
            Response(neb::ERR_OK, "OK");        // 操作是正常的，但结果集为空
            return(neb::CMD_STATUS_COMPLETED);
        }
    }

    // 从redis中读到数据
    neb::Result oRsp;
    oRsp.set_err_no(neb::ERR_OK);
    oRsp.set_err_msg("OK");
    oRsp.set_from(neb::Result::FROM_REDIS);
    if (REDIS_REPLY_STRING == pReply->type)
    {
        if (m_bIsDataSet)
        {
            neb::Record* pRecord = oRsp.add_record_data();
            if (!pRecord->ParseFromArray(pReply->str, pReply->len)) // 解析出错表明redis中数据有问题，需从db中读并覆盖redis中数据
            {
                if (m_pNextStep)    // redis中hash的某个Field数据为空（说明数据不完整），有下一步（通常是再尝试从DB中读）则执行下一步
                {
                    if (!m_pNextStep->Emit())
                    {
                        Response(neb::ERR_REDIS_NIL_AND_DB_FAILED,
                                        "redis result set is nil and send to dbagent failed!");
                        return(neb::CMD_STATUS_FAULT);
                    }
                    return(neb::CMD_STATUS_COMPLETED);
                }
                Response(neb::ERR_REDIS_NIL_AND_DB_FAILED,
                                "pRecord->ParseFromArray(pReply->element[i]->str, pReply->element[i]->len) failed!");
                return(neb::CMD_STATUS_FAULT);
            }
            if (m_iTableFieldNum > 0 && m_iTableFieldNum != pRecord->field_info_size())    // 字段数量不匹配表明redis中数据有问题，需从db中读并覆盖redis中数据
            {
                if (m_pNextStep)    // redis中hash的某个Field数据为空（说明数据不完整），有下一步（通常是再尝试从DB中读）则执行下一步
                {
                    if (!m_pNextStep->Emit())
                    {
                        Response(neb::ERR_REDIS_NIL_AND_DB_FAILED,
                                        "redis result set is nil and send to dbagent failed!");
                        return(neb::CMD_STATUS_FAULT);
                    }
                    return(neb::CMD_STATUS_COMPLETED);
                }
                Response(neb::ERR_REDIS_NIL_AND_DB_FAILED,
                                "the field of redis dataset record not match the db table field num!");
                return(neb::CMD_STATUS_FAULT);
            }
        }
        else
        {
            neb::Record* pRecord = oRsp.add_record_data();
            neb::Field* pField = pRecord->add_field_info();
            pField->set_col_value(pReply->str, pReply->len);
        }
    }
    else if(REDIS_REPLY_INTEGER == pReply->type)
    {
        neb::Record* pRecord = oRsp.add_record_data();
        neb::Field* pField = pRecord->add_field_info();
        char szValue[32] = {0};
        snprintf(szValue, 32, "%lld", pReply->integer);
        pField->set_col_value(szValue);
    }
    else if(REDIS_REPLY_ARRAY == pReply->type)
    {
        LOG4_TRACE("pReply->type = %d, pReply->elements = %u", pReply->type, pReply->elements);
        if (0 == pReply->elements)
        {
            if (m_pNextStep)    // redis中数据为空，有下一步（通常是再尝试从DB中读）则执行下一步
            {
                if (!m_pNextStep->Emit())
                {
                    Response(neb::ERR_REDIS_NIL_AND_DB_FAILED,
                                    "redis result set is nil and send to dbagent failed!");
                    return(neb::CMD_STATUS_FAULT);
                }
                return(neb::CMD_STATUS_COMPLETED);
            }
            else                // 只读redis，redis的结果又为空
            {
                Response(neb::ERR_OK, "OK");        // 操作是正常的，但结果集为空
                return(neb::CMD_STATUS_COMPLETED);
            }
        }
        if (REDIS_T_HASH == m_oRedisOperate.redis_structure())
        {
            if (ReadReplyHash(pReply))
            {
                return(neb::CMD_STATUS_COMPLETED);
            }
            return(neb::CMD_STATUS_FAULT);
        }
        else
        {
            if (ReadReplyArray(pReply))
            {
                return(neb::CMD_STATUS_COMPLETED);
            }
            return(neb::CMD_STATUS_FAULT);
        }
    }
    else
    {
        snprintf(szErrMsg, sizeof(szErrMsg), "unexprected redis reply type %d: %s!", pReply->type, pReply->str);
        Response(neb::ERR_UNEXPECTED_REDIS_REPLY, szErrMsg);
        return(neb::CMD_STATUS_FAULT);
    }

    if (Response(oRsp))
    {
        return(neb::CMD_STATUS_COMPLETED);
    }
    return(neb::CMD_STATUS_FAULT);
}

bool StepReadFromRedis::ReadReplyArray(redisReply* pReply)
{
    LOG4_TRACE("%s()", __FUNCTION__);
    if (m_bIsDataSet)
    {
        if (ReadReplyArrayWithDataSet(pReply))
        {
            return(true);
        }
        return(false);
    }
    else
    {
        if (ReadReplyArrayWithoutDataSet(pReply))
        {
            return(true);
        }
        return(false);
    }
}

bool StepReadFromRedis::ReadReplyArrayWithDataSet(redisReply* pReply)
{
    LOG4_TRACE("%s()", __FUNCTION__);
    char szErrMsg[256] = {0};
    neb::Result oRsp;
    oRsp.set_err_no(neb::ERR_OK);
    oRsp.set_err_msg("OK");
    oRsp.set_from(neb::Result::FROM_REDIS);
    oRsp.set_total_count(pReply->elements);
    oRsp.mutable_record_data()->Reserve(pReply->elements);
    int iDataLen = oRsp.ByteSize();
    for(size_t i = 0; i < pReply->elements; ++i)
    {
        if (pReply->element[i]->len > 1000000) // pb 最大限制
        {
            Response(neb::ERR_RESULTSET_EXCEED, "hgetall result set exceed 1 MB!");
            return(false);
        }
        if (iDataLen + pReply->element[i]->len > 1000000) // pb 最大限制
        {
            oRsp.set_current_count(i + 1);
            if (Response(oRsp))
            {
                oRsp.clear_record_data();
                iDataLen = 0;
            }
            else
            {
                return(false);
            }
        }

        neb::Record* pRecord = oRsp.add_record_data(); //.mutable_record_data(i);
        if(REDIS_REPLY_STRING == pReply->element[i]->type)
        {
//            google::protobuf::io::ArrayInputStream oArrayInput(pReply->element[i]->str, pReply->element[i]->len);
//            pRecord->ParseFromZeroCopyStream(&oArrayInput);
            if (!pRecord->ParseFromArray(pReply->element[i]->str, pReply->element[i]->len)) // 解析出错表明redis中数据有问题，需从db中读并覆盖redis中数据
            {
                if (m_pNextStep)    // redis中hash的某个Field数据为空（说明数据不完整），有下一步（通常是再尝试从DB中读）则执行下一步
                {
                    if (!m_pNextStep->Emit())
                    {
                        Response(neb::ERR_REDIS_NIL_AND_DB_FAILED,
                                        "redis result set is nil and send to dbagent failed!");
                        return(false);
                    }
                    return(true);
                }
                Response(neb::ERR_REDIS_NIL_AND_DB_FAILED,
                                "pRecord->ParseFromArray(pReply->element[i]->str, pReply->element[i]->len) failed!");
                return(false);
            }
            if (m_iTableFieldNum > 0 && m_iTableFieldNum != pRecord->field_info_size())    // 字段数量不匹配表明redis中数据有问题，需从db中读并覆盖redis中数据
            {
                if (m_pNextStep)    // redis中hash的某个Field数据为空（说明数据不完整），有下一步（通常是再尝试从DB中读）则执行下一步
                {
                    if (!m_pNextStep->Emit())
                    {
                        Response(neb::ERR_REDIS_NIL_AND_DB_FAILED,
                                        "redis result set is nil and send to dbagent failed!");
                        return(false);
                    }
                    return(true);
                }
                Response(neb::ERR_REDIS_NIL_AND_DB_FAILED,
                                "the field of redis dataset record not match the db table field num!");
                return(false);
            }
            iDataLen += pReply->element[i]->len;
        }
        else if(REDIS_REPLY_NIL == pReply->element[i]->type)
        {
            if (m_pNextStep)    // redis中hash的某个Field数据为空（说明数据不完整），有下一步（通常是再尝试从DB中读）则执行下一步
            {
                if (!m_pNextStep->Emit())
                {
                    Response(neb::ERR_REDIS_NIL_AND_DB_FAILED,
                                    "redis result set is nil and send to dbagent failed!");
                    return(false);
                }
                return(true);
            }
        }
        else
        {
            LOG4_ERROR("pReply->element[%d]->type = %d", i, pReply->element[i]->type);
            snprintf(szErrMsg, sizeof(szErrMsg), "unexprected redis reply type %d and element[%u] type %d: %s!",
                            pReply->type, i, pReply->element[i]->type, pReply->element[i]->str);
            Response(neb::ERR_UNEXPECTED_REDIS_REPLY, szErrMsg);
            return(false);
        }
    }

    oRsp.set_current_count(pReply->elements);
    if (Response(oRsp))
    {
        return(true);
    }
    return(false);
}

bool StepReadFromRedis::ReadReplyArrayWithoutDataSet(redisReply* pReply)
{
    LOG4_TRACE("%s()", __FUNCTION__);
    char szErrMsg[256] = {0};
    char szValue[32] = {0};
    neb::Result oRsp;
    oRsp.set_err_no(neb::ERR_OK);
    oRsp.set_err_msg("OK");
    oRsp.set_from(neb::Result::FROM_REDIS);
    oRsp.set_total_count(pReply->elements);
    int iDataLen = oRsp.ByteSize();
    for(size_t i = 0; i < pReply->elements; ++i)
    {
        if (pReply->element[i]->len > 1000000) // pb 最大限制
        {
            Response(neb::ERR_RESULTSET_EXCEED, "hgetall result set exceed 1 MB!");
            return(false);
        }
        if (iDataLen + pReply->element[i]->len > 1000000) // pb 最大限制
        {
            oRsp.set_current_count(i + 1);
            if (Response(oRsp))
            {
                oRsp.clear_record_data();
                iDataLen = 0;
            }
            else
            {
                return(false);
            }
        }
        neb::Record* pRecord = oRsp.add_record_data();
        neb::Field* pField = pRecord->add_field_info();
        if(REDIS_REPLY_STRING == pReply->element[i]->type)
        {
            pField->set_col_value(pReply->element[i]->str, pReply->element[i]->len);
            iDataLen += pReply->element[i]->len;
        }
        else if(REDIS_REPLY_INTEGER == pReply->element[i]->type)
        {
            snprintf(szValue, 32, "%lld",pReply->element[i]->integer);
            pField->set_col_value(szValue);
            iDataLen += 20;
        }
        else if(REDIS_REPLY_NIL == pReply->element[i]->type)
        {
            if (m_pNextStep)    // redis中hash的某个Field数据为空（说明数据不完整），有下一步（通常是再尝试从DB中读）则执行下一步
            {
                if (!m_pNextStep->Emit())
                {
                    Response(neb::ERR_REDIS_NIL_AND_DB_FAILED,
                                    "redis result set is nil and send to dbagent failed!");
                    return(false);
                }
                return(true);
            }
            else
            {
                pField->set_col_value("");
                LOG4_WARNING("pReply->element[%d]->type == REDIS_REPLY_NIL", i);
            }
        }
        else
        {
            LOG4_ERROR("pReply->element[%d]->type = %d", i, pReply->element[i]->type);
            snprintf(szErrMsg, sizeof(szErrMsg), "unexprected redis reply type %d and element[%u] type %d: %s!",
                            pReply->type, i, pReply->element[i]->type, pReply->element[i]->str);
            Response(neb::ERR_UNEXPECTED_REDIS_REPLY, szErrMsg);
            return(false);
        }
    }

    oRsp.set_current_count(pReply->elements);
    if (Response(oRsp))
    {
        return(true);
    }
    return(false);
}

bool StepReadFromRedis::ReadReplyHash(redisReply* pReply)
{
    LOG4_TRACE("%s()", __FUNCTION__);
    if ("HGETALL" == m_oRedisOperate.redis_cmd_read())
    {
        if (m_bIsDataSet)
        {
            if (ReadReplyArrayForHgetallWithDataSet(pReply))
            {
                return(true);
            }
            return(false);
        }
        else
        {
            if (ReadReplyArrayForHgetallWithoutDataSet(pReply))
            {
                return(true);
            }
            return(false);
        }
    }
    else
    {
        if (m_bIsDataSet)
        {
            if (ReadReplyArrayForHashWithDataSet(pReply))
            {
                return(true);
            }
            return(false);
        }
        else
        {
            if (ReadReplyArrayForHashWithoutDataSet(pReply))
            {
                return(true);
            }
            return(false);
        }
    }
}

bool StepReadFromRedis::ReadReplyArrayForHashWithDataSet(redisReply* pReply)
{
    LOG4_TRACE("%s()", __FUNCTION__);
    return(ReadReplyArrayWithDataSet(pReply));
}

bool StepReadFromRedis::ReadReplyArrayForHashWithoutDataSet(redisReply* pReply)
{
    LOG4_TRACE("%s()", __FUNCTION__);
    char szErrMsg[256] = {0};
    char szValue[32] = {0};
    neb::Result oRsp;
    oRsp.set_err_no(neb::ERR_OK);
    oRsp.set_err_msg("OK");
    oRsp.set_from(neb::Result::FROM_REDIS);
    oRsp.set_total_count(1);
    int iDataLen = oRsp.ByteSize();
    neb::Record* pRecord = oRsp.add_record_data();
    for(size_t i = 0; i < pReply->elements; ++i)
    {
        neb::Field* pField = pRecord->add_field_info();
        if(REDIS_REPLY_STRING == pReply->element[i]->type)
        {
            pField->set_col_value(pReply->element[i]->str, pReply->element[i]->len);
            iDataLen += pReply->element[i]->len;
        }
        else if(REDIS_REPLY_INTEGER == pReply->element[i]->type)
        {
            snprintf(szValue, 32, "%lld",pReply->element[i]->integer);
            pField->set_col_value(szValue);
            iDataLen += 20;
        }
        else if(REDIS_REPLY_NIL == pReply->element[i]->type)
        {
            if (m_pNextStep)    // redis中hash的某个Field数据为空（说明数据不完整），有下一步（通常是再尝试从DB中读）则执行下一步
            {
                if (!m_pNextStep->Emit())
                {
                    Response(neb::ERR_REDIS_NIL_AND_DB_FAILED,
                                    "redis result set is nil and send to dbagent failed!");
                    return(false);
                }
                return(true);
            }
            else
            {
                pField->set_col_value("");
                LOG4_WARNING("pReply->element[%d]->type == REDIS_REPLY_NIL", i);
            }
        }
        else
        {
            LOG4_ERROR("pReply->element[%d]->type = %d", i, pReply->element[i]->type);
            snprintf(szErrMsg, sizeof(szErrMsg), "unexprected redis reply type %d and element[%d] type %d: %s!",
                            pReply->type, i, pReply->element[i]->type, pReply->element[i]->str);
            Response(neb::ERR_UNEXPECTED_REDIS_REPLY, szErrMsg);
            return(false);
        }

        if (iDataLen > 1000000) // pb 最大限制
        {
            Response(neb::ERR_RESULTSET_EXCEED, "hash result set exceed 1 MB!");
            return(false);
        }
    }

    oRsp.set_current_count(1);
    if (Response(oRsp))
    {
        return(true);
    }
    return(false);
}

bool StepReadFromRedis::ReadReplyArrayForHgetallWithDataSet(redisReply* pReply)
{
    LOG4_TRACE("%s()", __FUNCTION__);
    char szErrMsg[256] = {0};
    neb::Result oRsp;
    oRsp.set_err_no(neb::ERR_OK);
    oRsp.set_err_msg("OK");
    oRsp.set_from(neb::Result::FROM_REDIS);
    int iDataLen = oRsp.ByteSize();
    if ((pReply->elements % 2) != 0)
    {
        snprintf(szErrMsg, sizeof(szErrMsg), "unexprected redis reply type %d elements num %u not a even number for hgetall!",
                        pReply->type, pReply->elements);
        Response(neb::ERR_UNEXPECTED_REDIS_REPLY, szErrMsg);
        return(neb::CMD_STATUS_FAULT);
    }
    oRsp.set_total_count(pReply->elements);
    neb::Record* pHashFieldNameRecord = oRsp.add_record_data();
    neb::Record* pHashFieldValueRecord = oRsp.add_record_data();
    for(size_t i = 0, j = 1; i < pReply->elements; i += 2, j = i + 1)
    {
        if (pReply->element[i]->len > 1000000) // pb 最大限制
        {
            Response(neb::ERR_RESULTSET_EXCEED, "hgetall result set exceed 1 MB!");
            return(false);
        }
        if (iDataLen + pReply->element[i]->len > 1000000) // pb 最大限制
        {
            oRsp.set_current_count(i + 1);
            if (Response(oRsp))
            {
                oRsp.clear_record_data();
                iDataLen = 0;
            }
            else
            {
                return(false);
            }
        }
        neb::Field* pField = pHashFieldNameRecord->add_field_info();
        if(REDIS_REPLY_STRING == pReply->element[i]->type)
        {
            pField->set_col_name(pReply->element[i]->str, pReply->element[i]->len);
            pField->set_col_value(pReply->element[i]->str, pReply->element[i]->len);
            iDataLen += pReply->element[i]->len * 2;
        }
        else
        {
            LOG4_ERROR("pReply->element[%d]->type = %d", i, pReply->element[i]->type);
            snprintf(szErrMsg, sizeof(szErrMsg), "unexprected redis reply type %d and element[%d] type %d: %s!",
                            pReply->type, i, pReply->element[i]->type, pReply->element[i]->str);
            Response(neb::ERR_UNEXPECTED_REDIS_REPLY, szErrMsg);
            return(false);
        }
        if(REDIS_REPLY_STRING == pReply->element[j]->type)
        {
            pHashFieldValueRecord->ParseFromArray(pReply->element[j]->str, pReply->element[j]->len);
            iDataLen += pReply->element[j]->len;
        }
        else if(REDIS_REPLY_NIL == pReply->element[i]->type)
        {
            if (m_pNextStep)    // redis中hash的某个Field数据为空（说明数据不完整），有下一步（通常是再尝试从DB中读）则执行下一步
            {
                if (!m_pNextStep->Emit())
                {
                    Response(neb::ERR_REDIS_NIL_AND_DB_FAILED,
                                    "redis result set is nil and send to dbagent failed!");
                    return(false);
                }
                return(true);
            }
        }
        else
        {
            LOG4_ERROR("pReply->element[%d]->type = %d", j, pReply->element[j]->type);
            snprintf(szErrMsg, sizeof(szErrMsg), "unexprected redis reply type %d and element[%d] type %d: %s!",
                            pReply->type, j, pReply->element[j]->type, pReply->element[j]->str);
            Response(neb::ERR_UNEXPECTED_REDIS_REPLY, szErrMsg);
            return(false);
        }
    }

    oRsp.set_current_count(pReply->elements);
    if (Response(oRsp))
    {
        return(true);
    }
    return(false);
}

bool StepReadFromRedis::ReadReplyArrayForHgetallWithoutDataSet(redisReply* pReply)
{
    LOG4_TRACE("%s()", __FUNCTION__);
    char szErrMsg[256] = {0};
    char szValue[32] = {0};
    neb::Result oRsp;
    oRsp.set_err_no(neb::ERR_OK);
    oRsp.set_err_msg("OK");
    oRsp.set_from(neb::Result::FROM_REDIS);
    int iDataLen = oRsp.ByteSize();
    if ((pReply->elements % 2) != 0)
    {
        snprintf(szErrMsg, sizeof(szErrMsg), "unexprected redis reply type %d elements num %u not a even number for hgetall!",
                        pReply->type, pReply->elements);
        Response(neb::ERR_UNEXPECTED_REDIS_REPLY, szErrMsg);
        return(neb::CMD_STATUS_FAULT);
    }
    oRsp.set_total_count(1);
    neb::Record* pRecord = oRsp.add_record_data();
    for(size_t i = 0, j = 1; i < pReply->elements; i += 2, j = i + 1)
    {
        neb::Field* pField = pRecord->add_field_info();
        if(REDIS_REPLY_STRING == pReply->element[i]->type)
        {
            pField->set_col_name(pReply->element[i]->str, pReply->element[i]->len);
            iDataLen += pReply->element[i]->len;
        }
        else if(REDIS_REPLY_NIL == pReply->element[i]->type)
        {
            if (m_pNextStep)    // redis中hash的某个Field数据为空（说明数据不完整），有下一步（通常是再尝试从DB中读）则执行下一步
            {
                if (!m_pNextStep->Emit())
                {
                    Response(neb::ERR_REDIS_NIL_AND_DB_FAILED,
                                    "redis result set is nil and send to dbagent failed!");
                    return(false);
                }
                return(true);
            }
        }
        else
        {
            LOG4_ERROR("pReply->element[%d]->type = %d", i, pReply->element[i]->type);
            snprintf(szErrMsg, sizeof(szErrMsg), "unexprected redis reply type %d and element[%u] type %d: %s!",
                            pReply->type, i, pReply->element[i]->type, pReply->element[i]->str);
            Response(neb::ERR_UNEXPECTED_REDIS_REPLY, szErrMsg);
            return(false);
        }
        if(REDIS_REPLY_STRING == pReply->element[j]->type)
        {
            pField->set_col_value(pReply->element[j]->str, pReply->element[j]->len);
            iDataLen += pReply->element[j]->len;
        }
        else if(REDIS_REPLY_INTEGER == pReply->element[j]->type)
        {
            snprintf(szValue, 32, "%lld",pReply->element[j]->integer);
            pField->set_col_value(szValue);
            iDataLen += 20;
        }
        else if(REDIS_REPLY_NIL == pReply->element[i]->type)
        {
            if (m_pNextStep)    // redis中hash的某个Field数据为空（说明数据不完整），有下一步（通常是再尝试从DB中读）则执行下一步
            {
                if (!m_pNextStep->Emit())
                {
                    Response(neb::ERR_REDIS_NIL_AND_DB_FAILED,
                                    "redis result set is nil and send to dbagent failed!");
                    return(false);
                }
                return(true);
            }
            else
            {
                pField->set_col_value("");
                LOG4_WARNING("pReply->element[%d]->type == REDIS_REPLY_NIL", i);
            }
        }
        else
        {
            LOG4_ERROR("pReply->element[%d]->type = %d", j, pReply->element[j]->type);
            snprintf(szErrMsg, sizeof(szErrMsg), "unexprected redis reply type %d and element[%u] type %d: %s!",
                            pReply->type, j, pReply->element[j]->type, pReply->element[j]->str);
            Response(neb::ERR_UNEXPECTED_REDIS_REPLY, szErrMsg);
            return(false);
        }

        if (iDataLen > 1000000) // pb 最大限制
        {
            Response(neb::ERR_RESULTSET_EXCEED, "hgetall result set exceed 1 MB!");
            return(false);
        }
    }

    oRsp.set_current_count(1);
    if (Response(oRsp))
    {
        return(true);
    }
    return(false);
}
} /* namespace mydis */
