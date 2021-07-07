# Copyright (C) 2018 Lecida Inc
# All Rights Reserved.
#
# NOTICE:  All information contained herein is, and remains the property of
# Lecida Inc. The intellectual and technical concepts contained herein are
# proprietary to Lecida Inc and may be covered by U.S. and Foreign Patents,
# patents in process, and are protected by trade secret or copyright law.
# Dissemination or reproduction of this material is strictly forbidden unless
# prior written permission is obtained from Lecida Inc.
"""Yaml util. classes."""
from __future__ import annotations

import dataclasses as dc
import logging
from functools import partial
from typing import Any, Dict, Optional, Type
from uuid import UUID, uuid5

import yaml

logger = logging.getLogger(__name__)

LECIDA_YAML_UUID_NAMESPACE = UUID(int=0x7ec1daeda7c85497884b838a21f1b17e,
                                  version=5)


class Loader(yaml.SafeLoader):
    """YAML safe loader with custom Lecida tags."""

    ...


@dc.dataclass
class _Constructor():
    """YAML Constuctor for EDA classes.

    Contrary to usual YAML constructors, it saves the name of the target class.

    Attributes:
        target_cls: The target class. An instance of this class will be created
            every time the constructor is called.

    """

    target_cls: Type[YAMLRegistered]

    @classmethod
    def _parameter_representation(cls, value: Any, deep: bool = True) -> Any:
        """Replace YAMLRegistered by their hashes in a repr.

        Args:
            deep: Whether to apply this function recursively in lists and
                dicts.

        """
        if isinstance(value, YAMLRegistered):
            return f'{value.input_yaml_tag}#{value.input_hash}'

        if deep:
            if isinstance(value, list):
                return [cls._parameter_representation(v) for v in value]
            if isinstance(value, dict):
                return {k: cls._parameter_representation(v)
                        for k, v in value.items()}
        return value

    def __call__(self, loader: yaml.SafeLoader, node: yaml.Node) \
            -> YAMLRegistered:
        """YAML constructor for this subclass."""
        if not isinstance(loader, Loader):
            raise TypeError(f'loader should be an instance of '
                            f'sriracha.yaml.Loader, but fot {loader}.')
        if isinstance(node, yaml.MappingNode):
            kwargs = loader.construct_mapping(node, deep=True)
        elif isinstance(node, yaml.ScalarNode) and not node.value:
            if node.value:
                raise ValueError('A non-empty YAML scalar node cannot be used,'
                                 f'but got {node.value}.')
            else:
                kwargs = {}
        else:
            raise TypeError('The YAML node must be a mapping or a scalar, but '
                            f'got {node}.')

        instance = self.target_cls.__new__(  # type: ignore
            self.target_cls, **kwargs
        )

        instance._yaml_input_node = node
        instance._yaml_input_params = self._parameter_representation(kwargs)

        try:
            instance.__init__(**kwargs)
        except TypeError:
            raise TypeError(f'Error while initializing instance {instance} '
                            f'using YAML node {node}.')

        return instance


class YAMLRegistered(object):
    """Object that is automatically registered for YAML EDA config. files."""

    _yaml_input_node: yaml.nodes.Node
    _yaml_input_params: Dict[str, Any]

    def __init_subclass__(cls, register_yaml: bool = True,
                          yaml_tag: Optional[str] = None) -> None:
        """Register a YAML object.

        Args:
            register_yaml: Whether to register the class for YAML processing.
                Defaults to True. This doesn't impact the subclasses.
            yaml_tag: The YAML tag. If None, cls.format_yaml_tag will be called
                instead. Ignored if register_yaml is False.

        """
        if register_yaml:
            if yaml_tag is None:
                try:
                    yaml_tag = cls.format_yaml_tag(subcls=cls)
                except NotImplementedError:
                    raise ValueError(f'No YAML tag for {cls}, with '
                                     'register_yaml=True')

            cls.check_yaml_tag(cls, yaml_tag)

            # Check uniqueness of the YAML tag
            existing_constructor = (Loader
                                    .yaml_constructors
                                    .get(yaml_tag))
            if existing_constructor is not None:
                if isinstance(existing_constructor, _Constructor):
                    logger.warning(
                        f'YAML Tag {yaml_tag} is already defined. Replacing '
                        f'existing constructor (target class = '
                        f'{existing_constructor.target_cls})'
                    )
                else:
                    raise ValueError(
                        f'YAML Tag {yaml_tag} is already defined, and the '
                        f'constructor ({existing_constructor}) is not a '
                        f'Lecida Constructor.'
                    )

            constructor = _Constructor(target_cls=cls)
            Loader.add_constructor(tag=yaml_tag,
                                   constructor=constructor)

            logger.debug(f'Registered YAML Tag {yaml_tag} for {cls}.')

        super().__init_subclass__()

    @classmethod
    def check_yaml_tag(cls, subcls, yaml_tag: str) -> None:
        """Check that a YAML tag is correct for the given subclass.

        This method can be overridden.
        """
        if not yaml_tag.startswith('!'):
            raise ValueError(f'YAML tag should start with "!", but {yaml_tag} '
                             f'doesn\'t')

    @staticmethod
    def format_yaml_tag(subcls) -> str:
        """Format a tag for a subclass.

        Args:
            subcls: The subclass.

        Returns:
            The YAML tag associated with the subclass.

        """
        raise NotImplementedError

    @property
    def input_yaml_tag(self) -> str:
        """Return the input tag the YAML object."""
        return self._yaml_input_node.tag

    @property
    def input_parameters(self) -> Dict[str, Any]:
        """Return the input parameters used to create the YAML object."""
        return self._yaml_input_params

    @property
    def input_hash(self) -> str:
        """Return the hash of the YAML input node."""
        return str(uuid5(namespace=LECIDA_YAML_UUID_NAMESPACE,
                         name=repr(self._yaml_input_node)))

    def __repr__(self) -> str:
        """Return the representation of the object."""
        params_string = ','.join(
            f'{k}={v}' for k, v in sorted(self._yaml_input_params.items())
        )
        return f'{self.input_yaml_tag}#{self.input_hash}({params_string})'

    def __str__(self) -> str:
        """Return the short representation of the object."""
        return f'{self.input_yaml_tag}#{self.input_hash[:8]}'


class Dumper(yaml.SafeDumper):
    """YAML Dumper that can represent any Lecida YAML registered object."""

    def __init__(self, *args, **kwargs) -> None:
        """Initialize the YAML dumper."""
        super().__init__(*args, **kwargs)
        # Add a representer for YAMLRegistered and all its
        # subclasses
        self.add_multi_representer(
            data_type=YAMLRegistered,
            representer=lambda _, obj: obj._yaml_input_node
        )


def get_lecida_yaml_tag_mapping(filter_prefix: str = '') \
        -> Dict[str, Type[YAMLRegistered]]:
    """Return a mapping YAML tag → Corresponding target class.

    Args:
        filter_prefix: The filter for the prefix of the tag.

    Returns:
        The YAML tag → Corresponding target class mapping.

    """
    return {
        yaml_tag: constructor.target_cls
        for yaml_tag, constructor in yaml.SafeLoader.yaml_constructors.items()
        if (isinstance(constructor, _Constructor)
            and yaml_tag.startswith(filter_prefix))
    }


load = partial(yaml.load, Loader=Loader)
dump = partial(yaml.dump, Dumper=Dumper)
