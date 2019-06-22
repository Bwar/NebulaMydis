/*******************************************************************************
 * Project:  NebulaMydis
 * @file     ContextRequest.cpp
 * @brief 
 * @author   Bwar
 * @date:    2019年2月17日
 * @note
 * Modify history:
 ******************************************************************************/
#include "ContextRequest.hpp"

namespace mydis
{

ContextRequest::ContextRequest(
        std::shared_ptr<neb::SocketChannel> pChannel, int32 iCmd, uint32 uiSeq, const MsgBody& oMsgBody)
    : neb::PbContext(pChannel, iCmd, uiSeq, oMsgBody)
{
}

ContextRequest::~ContextRequest()
{
}

} /* namespace mydis */
