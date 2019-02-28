/*******************************************************************************
 * Project:  NebulaMydis
 * @file     StepSetTtl.cpp
 * @brief 
 * @author   Bwar
 * @date:    2018年3月3日
 * @note
 * Modify history:
 ******************************************************************************/
#include "StepSetTtl.hpp"

namespace mydis
{

StepSetTtl::StepSetTtl(const std::string& strMasterNodeIdentify, const std::string& strKey, int32 iExpireSeconds)
    : m_strMasterNodeIdentify(strMasterNodeIdentify), m_strKey(strKey), m_iExpireSeconds(iExpireSeconds)
{
}

StepSetTtl::~StepSetTtl()
{
}

neb::E_CMD_STATUS StepSetTtl::Emit(int iErrno, const std::string& strErrMsg, void* data)
{
    LOG4_TRACE("%s()", __FUNCTION__);
    SetCmd("EXPIRE");
    Append(m_strKey);
    Append(std::to_string(m_iExpireSeconds));
    if (SendTo(m_strMasterNodeIdentify))
    {
        return(neb::CMD_STATUS_RUNNING);
    }
    LOG4_ERROR("RegisterCallback(%s, StepSetTtl) error!", m_strMasterNodeIdentify.c_str());
    return(neb::CMD_STATUS_FAULT);
}

neb::E_CMD_STATUS StepSetTtl::Callback(const redisAsyncContext *c, int status, redisReply* pReply)
{
    LOG4_TRACE("%s()", __FUNCTION__);
    if (REDIS_OK != status)
    {
        LOG4_ERROR("redis cmd status %d!", status);
        return(neb::CMD_STATUS_FAULT);
    }
    if (NULL == pReply)
    {
        LOG4_ERROR("error %d: %s!", c->err, c->errstr);
        return(neb::CMD_STATUS_FAULT);
    }
    LOG4_TRACE("redis reply->type = %d", pReply->type);
    if (REDIS_REPLY_ERROR == pReply->type)
    {
        LOG4_ERROR("error %d: %s!", pReply->type, pReply->str);
        return(neb::CMD_STATUS_FAULT);
    }
    LOG4_TRACE("OK");
    return(neb::CMD_STATUS_COMPLETED);
}

} /* namespace mydis */
