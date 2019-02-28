/*******************************************************************************
 * Project:  NebulaMydis
 * @file     SessionRedisNode.cpp
 * @brief 
 * @author   Bwar
 * @date:    2018年3月1日
 * @note
 * Modify history:
 ******************************************************************************/
#define CRYPTOPP_ENABLE_NAMESPACE_WEAK 1
#include "cryptopp/md5.h"
//#include "cryptopp/hex.h"
#include "SessionRedisNode.hpp"

namespace mydis
{

SessionRedisNode::SessionRedisNode(const std::string& strSessionId,
                int iHashAlgorithm, int iVirtualNodeNum, ev_tstamp dSessionTimeout)
    : neb::Session(strSessionId, dSessionTimeout),
      m_iHashAlgorithm(iHashAlgorithm), m_iVirtualNodeNum(iVirtualNodeNum)
{
}

SessionRedisNode::~SessionRedisNode()
{
    m_mapRedisNode.clear();
    m_mapRedisNodeHash.clear();
}

bool SessionRedisNode::GetRedisNode(const std::string& strHashKey, std::string& strMasterNodeIdentify, std::string& strSlaveNodeIdentify)
{
    uint32 uiKeyHash = 0;
    if (HASH_fnv1_64 == m_iHashAlgorithm)
    {
        uiKeyHash = hash_fnv1_64(strHashKey.c_str(), strHashKey.size());
    }
    else if (HASH_murmur3_32 == m_iHashAlgorithm)
    {
        uiKeyHash = murmur3_32(strHashKey.c_str(), strHashKey.size(), 0x000001b3);
    }
    else
    {
        uiKeyHash = hash_fnv1a_64(strHashKey.c_str(), strHashKey.size());
    }
//    byte szDigest[CryptoPP::CRC32::DIGESTSIZE] = {0};
//    m_oCrc32.CalculateDigest(szDigest, (const byte*)strHashKey.c_str(), (unsigned long int)strHashKey.size());
//    uiKeyHash = *(uint32_t*)szDigest;
    std::map<uint32, std::pair<std::string, std::string> >::const_iterator c_iter = m_mapRedisNodeHash.lower_bound(uiKeyHash);
    if (c_iter == m_mapRedisNodeHash.end())
    {
        strMasterNodeIdentify = m_mapRedisNodeHash.begin()->second.first;
        strSlaveNodeIdentify = m_mapRedisNodeHash.begin()->second.second;
    }
    else
    {
        strMasterNodeIdentify = c_iter->second.first;
        strSlaveNodeIdentify = c_iter->second.second;
    }
    return(true);
}

void SessionRedisNode::AddRedisNode(const std::string& strNodeIdentify,
                const std::string& strMasterHostPort,
                const std::string& strSlaveHostPort)
{
    LOG4_DEBUG("%s()", __FUNCTION__);
    std::map<std::string, tagRedisNodeAttr* >::iterator node_iter = m_mapRedisNode.find(strNodeIdentify);
    if (node_iter == m_mapRedisNode.end())
    {
        /*
#define CRYPTOPP_ENABLE_NAMESPACE_WEAK 1
#include <md5.h>

byte digest[ CryptoPP::Weak::MD5::DIGESTSIZE ];
std::string message = "abcdefghijklmnopqrstuvwxyz";

CryptoPP::Weak::MD5 hash;
hash.CalculateDigest( digest, (const byte*)message.c_str(), message.length() );

CryptoPP::HexEncoder encoder;
std::string output;

encoder.Attach( new CryptoPP::StringSink( output ) );
encoder.Put( digest, sizeof(digest) );
encoder.MessageEnd();

std::cout << output << std::endl;
        */
        CryptoPP::Weak::MD5 oMd5;
        char szVirtualNodeIdentify[32] = {0};
        CryptoPP::byte szDigest[CryptoPP::Weak::MD5::DIGESTSIZE] = {0};
        int32 iPointPerHash = 4;
        tagRedisNodeAttr* pRedisNodeAttr = new tagRedisNodeAttr();
        pRedisNodeAttr->strMasterNodeIdentify = strMasterHostPort;
        pRedisNodeAttr->strSlaveNodeIdentify = strSlaveHostPort;
        for (int i = 0; i < m_iVirtualNodeNum / iPointPerHash; ++i)     // distribution: ketama
        {
            snprintf(szVirtualNodeIdentify, 32, "%s#%d", strNodeIdentify.c_str(), i);
            oMd5.CalculateDigest(szDigest, (const CryptoPP::byte*)szVirtualNodeIdentify, strlen(szVirtualNodeIdentify));
//            uiHashValue = *(uint32_t*)szDigest;
            for (int j = 0; j < iPointPerHash; ++j)
            {
                uint32 k = ((uint32)(szDigest[3 + j * iPointPerHash] & 0xFF) << 24)
                       | ((uint32)(szDigest[2 + j * iPointPerHash] & 0xFF) << 16)
                       | ((uint32)(szDigest[1 + j * iPointPerHash] & 0xFF) << 8)
                       | (szDigest[j * iPointPerHash] & 0xFF);
                pRedisNodeAttr->vecHash.push_back(k);
                LOG4_DEBUG("uiHashValue = %u, szVirtualNodeIdentify = %s", k, szVirtualNodeIdentify);
                m_mapRedisNodeHash.insert(std::pair<uint32, std::pair<std::string, std::string> >(k,
                                std::make_pair(strMasterHostPort, strSlaveHostPort) ));
            }
        }
        m_mapRedisNode.insert(std::pair<std::string, tagRedisNodeAttr*>(strNodeIdentify, pRedisNodeAttr));
    }
    else
    {
        node_iter->second->strMasterNodeIdentify = strMasterHostPort;
        node_iter->second->strSlaveNodeIdentify = strSlaveHostPort;
        std::map<uint32, std::pair<std::string, std::string> >::iterator node_hash_iter;
        for (std::vector<uint32>::iterator hash_iter = node_iter->second->vecHash.begin();
                        hash_iter != node_iter->second->vecHash.end(); ++hash_iter)
        {
            node_hash_iter = m_mapRedisNodeHash.find(*hash_iter);
            if (node_hash_iter != m_mapRedisNodeHash.end())
            {
                node_hash_iter->second = std::make_pair(strMasterHostPort, strSlaveHostPort);
            }
        }
    }
}

void SessionRedisNode::DelRedisNode(const std::string& strNodeIdentify)
{
    LOG4_DEBUG("%s()", __FUNCTION__);
    std::map<std::string, tagRedisNodeAttr* >::iterator node_iter = m_mapRedisNode.find(strNodeIdentify);
    if (node_iter != m_mapRedisNode.end())
    {
        for (std::vector<uint32>::iterator hash_iter = node_iter->second->vecHash.begin();
                        hash_iter != node_iter->second->vecHash.end(); ++hash_iter)
        {
            m_mapRedisNodeHash.erase(m_mapRedisNodeHash.find(*hash_iter));
        }
        delete node_iter->second;
        m_mapRedisNode.erase(node_iter);
    }
}

uint32 SessionRedisNode::hash_fnv1_64(const char *key, size_t key_length)
{
    uint64_t hash = FNV_64_INIT;
    size_t x;

    for (x = 0; x < key_length; x++) {
      hash *= FNV_64_PRIME;
      hash ^= (uint64_t)key[x];
    }

    return (uint32_t)hash;
}

uint32 SessionRedisNode::hash_fnv1a_64(const char *key, size_t key_length)
{
    uint32_t hash = (uint32_t) FNV_64_INIT;
    size_t x;

    for (x = 0; x < key_length; x++) {
      uint32_t val = (uint32_t)key[x];
      hash ^= val;
      hash *= (uint32_t) FNV_64_PRIME;
    }

    return hash;
}

uint32_t SessionRedisNode::murmur3_32(const char *key, uint32_t len, uint32_t seed)
{
    static const uint32_t c1 = 0xcc9e2d51;
    static const uint32_t c2 = 0x1b873593;
    static const uint32_t r1 = 15;
    static const uint32_t r2 = 13;
    static const uint32_t m = 5;
    static const uint32_t n = 0xe6546b64;

    uint32_t hash = seed;

    const int nblocks = len / 4;
    const uint32_t *blocks = (const uint32_t *) key;
    int i;
    uint32_t k;
    for (i = 0; i < nblocks; i++)
    {
        k = blocks[i];
        k *= c1;
        k = ROT32(k, r1);
        k *= c2;

        hash ^= k;
        hash = ROT32(hash, r2) * m + n;
    }

    const uint8_t *tail = (const uint8_t *) (key + nblocks * 4);
    uint32_t k1 = 0;

    switch (len & 3)
    {
    case 3:
        k1 ^= tail[2] << 16;
    case 2:
        k1 ^= tail[1] << 8;
    case 1:
        k1 ^= tail[0];

        k1 *= c1;
        k1 = ROT32(k1, r1);
        k1 *= c2;
        hash ^= k1;
    }

    hash ^= len;
    hash ^= (hash >> 16);
    hash *= 0x85ebca6b;
    hash ^= (hash >> 13);
    hash *= 0xc2b2ae35;
    hash ^= (hash >> 16);

    return hash;
}

} /* namespace mydis */
