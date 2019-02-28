/*******************************************************************************
 * Project:  NebulaMydis
 * @file     DbStorageStep.cpp
 * @brief 
 * @author   Bwar
 * @date:    2018年3月2日
 * @note
 * Modify history:
 ******************************************************************************/
#include "DbStorageStep.hpp"

namespace mydis
{

DbStorageStep::DbStorageStep(std::shared_ptr<neb::Step> pNextStep)
    : neb::PbStep(pNextStep)
{
}

DbStorageStep::~DbStorageStep()
{
}

bool DbStorageStep::Response(int iErrno, const std::string& strErrMsg)
{
    LOG4_TRACE("%d: %s", iErrno, strErrMsg.c_str());
    if (nullptr == GetContext())
    {
        return(false);
    }
    return(GetContext()->Response(iErrno, strErrMsg));
}

bool DbStorageStep::Response(const neb::Result& oRsp)
{
    LOG4_TRACE("%d: %s", oRsp.err_no(), oRsp.err_msg().c_str());
    if (nullptr == GetContext())
    {
        return(false);
    }
    return(GetContext()->Response(oRsp.err_no(), oRsp.SerializeAsString()));
}


} /* namespace mydis */
