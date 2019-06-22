/*******************************************************************************
 * Project:  NebulaMydis
 * @file     ContextRequest.hpp
 * @brief    网络请求上下文信息
 * @author   Bwar
 * @date:    2019年2月17日
 * @note     
 * Modify history:
 ******************************************************************************/
#ifndef MYDIS_CONTEXTREQUEST_HPP_
#define MYDIS_CONTEXTREQUEST_HPP_

#include "actor/context/PbContext.hpp"

namespace mydis
{

class ContextRequest: public neb::PbContext,
    public neb::DynamicCreator<ContextRequest,
                               std::shared_ptr<neb::SocketChannel>,
                               int32, uint32, MsgBody>
{
public:
    ContextRequest(
            std::shared_ptr<neb::SocketChannel> pChannel,
            int32 iCmd, uint32 uiSeq,
            const MsgBody& oMsgBody);
    virtual ~ContextRequest();
};

} /* namespace mydis */

#endif /* MYDIS_CONTEXTREQUEST_HPP_ */
