from zope.interface import Interface, Attribute

class IThriftAMQClientFactory(Interface):

    iprot_factory = Attribute("Input protocol factory")
    oprot_factory = Attribute("Input protocol factory")
    processor = Attribute("Thrift processor")
