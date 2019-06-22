/*******************************************************************************
 * Project:  NebulaMydis
 * @file     StorageStep.cpp
 * @brief 
 * @author   Bwar
 * @date:    2018年3月2日
 * @note
 * Modify history:
 ******************************************************************************/
#include "RedisStorageStep.hpp"
#include <actor/context/PbContext.hpp>

namespace mydis
{

RedisStorageStep::RedisStorageStep(std::shared_ptr<neb::Step> pNextStep)
    : neb::RedisStep(pNextStep)
{
}

RedisStorageStep::~RedisStorageStep()
{
}

bool RedisStorageStep::Response(int iErrno, const std::string& strErrMsg)
{
    LOG4_TRACE("%d: %s", iErrno, strErrMsg.c_str());
    if (nullptr == GetContext())
    {
        return(false);
    }
    std::shared_ptr<neb::PbContext> pSharedContext = std::dynamic_pointer_cast<neb::PbContext>(GetContext());
    return(pSharedContext->Response(iErrno, strErrMsg));
}

bool RedisStorageStep::Response(const neb::Result& oRsp)
{
    LOG4_TRACE("%d: %s", oRsp.err_no(), oRsp.err_msg().c_str());
    if (nullptr == GetContext())
    {
        return(false);
    }
    std::shared_ptr<neb::PbContext> pSharedContext = std::dynamic_pointer_cast<neb::PbContext>(GetContext());
    return(pSharedContext->Response(oRsp.err_no(), oRsp.SerializeAsString()));
}

} /* namespace mydis */
